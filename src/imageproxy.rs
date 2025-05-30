//! Run skopeo as a subprocess to fetch container images.
//!
//! This allows fetching a container image manifest and layers in a streaming fashion.
//!
//! More information: <https://github.com/containers/skopeo/pull/1476>

use cap_std_ext::prelude::CapStdExtCommandExt;
use cap_std_ext::{cap_std, cap_tempfile};
use futures_util::{Future, FutureExt};
use itertools::Itertools;
use oci_spec::image::{Descriptor, Digest};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::iter::FusedIterator;
use std::num::NonZeroU32;
use std::ops::Range;
use std::os::fd::OwnedFd;
use std::os::unix::prelude::CommandExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncReadExt};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinError;
use tracing::instrument;

/// Errors returned by this crate.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("i/o error")]
    /// An input/output error
    Io(#[from] std::io::Error),
    #[error("skopeo spawn error: {}", .0)]
    /// An error spawning skopeo
    SkopeoSpawnError(#[source] std::io::Error),
    #[error("serialization error")]
    /// Returned when serialization or deserialization fails
    SerDe(#[from] serde_json::Error),
    /// The proxy failed to initiate a request
    #[error("failed to invoke method {method}: {error}")]
    RequestInitiationFailure { method: Box<str>, error: Box<str> },
    /// An error returned from the remote proxy
    #[error("proxy request returned error")]
    RequestReturned(Box<str>),
    #[error("semantic version error")]
    SemanticVersion(#[from] semver::Error),
    #[error("proxy too old (requested={requested_version} found={found_version}) error")]
    /// The proxy doesn't support the requested semantic version
    ProxyTooOld {
        requested_version: Box<str>,
        found_version: Box<str>,
    },
    #[error("configuration error")]
    /// Conflicting or missing configuration
    Configuration(Box<str>),
    #[error("error")]
    /// An unknown other error
    Other(Box<str>),
}

impl Error {
    pub(crate) fn new_other(e: impl Into<Box<str>>) -> Self {
        Self::Other(e.into())
    }
}

/// Errors returned by get_raw_blob
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum GetBlobError {
    /// A client may reasonably retry on this type of error.
    #[error("retryable error")]
    Retryable(Box<str>),
    #[error("error")]
    /// An unknown other error
    Other(Box<str>),
}

impl From<rustix::io::Errno> for Error {
    fn from(value: rustix::io::Errno) -> Self {
        Self::Io(value.into())
    }
}

/// The error type returned from this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Re-export because we use this in our public APIs
pub use oci_spec;

/// File descriptor range which is reserved for passing data down into the proxy;
/// avoid configuring the command to use files in this range.  (Also, stdin is
/// reserved)
pub const RESERVED_FD_RANGE: Range<i32> = 100..200;

// This is defined in skopeo; maximum size of JSON we will read/write.
// Note that payload data (non-metadata) should go over a pipe file descriptor.
const MAX_MSG_SIZE: usize = 32 * 1024;

fn base_proto_version() -> &'static semver::VersionReq {
    // Introduced in https://github.com/containers/skopeo/pull/1523
    static BASE_PROTO_VERSION: OnceLock<semver::VersionReq> = OnceLock::new();
    BASE_PROTO_VERSION.get_or_init(|| semver::VersionReq::parse("0.2.3").unwrap())
}

fn layer_info_proto_version() -> &'static semver::VersionReq {
    static LAYER_INFO_PROTO_VERSION: OnceLock<semver::VersionReq> = OnceLock::new();
    LAYER_INFO_PROTO_VERSION.get_or_init(|| semver::VersionReq::parse("0.2.5").unwrap())
}

fn layer_info_piped_proto_version() -> &'static semver::VersionReq {
    static LAYER_INFO_PROTO_VERSION: OnceLock<semver::VersionReq> = OnceLock::new();
    LAYER_INFO_PROTO_VERSION.get_or_init(|| semver::VersionReq::parse("0.2.7").unwrap())
}

#[derive(Serialize)]
struct Request {
    method: String,
    args: Vec<serde_json::Value>,
}

impl Request {
    fn new<T, I>(method: &str, args: T) -> Self
    where
        T: IntoIterator<Item = I>,
        I: Into<serde_json::Value>,
    {
        let args: Vec<_> = args.into_iter().map(|v| v.into()).collect();
        Self {
            method: method.to_string(),
            args,
        }
    }

    fn new_bare(method: &str) -> Self {
        Self {
            method: method.to_string(),
            args: vec![],
        }
    }
}

#[derive(Deserialize)]
struct Reply {
    success: bool,
    error: String,
    pipeid: u32,
    value: serde_json::Value,
}

type ChildFuture = Pin<
    Box<
        dyn Future<Output = std::result::Result<std::io::Result<std::process::Output>, JoinError>>
            + Send,
    >,
>;

/// Manage a child process proxy to fetch container images.
pub struct ImageProxy {
    sockfd: Arc<Mutex<OwnedFd>>,
    childwait: Arc<AsyncMutex<ChildFuture>>,
    protover: semver::Version,
}

impl std::fmt::Debug for ImageProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageProxy").finish()
    }
}

/// Opaque identifier for an image
#[derive(Debug, PartialEq, Eq)]
pub struct OpenedImage(u32);

#[derive(Debug, PartialEq, Eq)]
struct PipeId(NonZeroU32);

impl PipeId {
    fn try_new(pipeid: u32) -> Option<Self> {
        Some(Self(NonZeroU32::new(pipeid)?))
    }
}

/// Configuration for the proxy.
#[derive(Debug, Default)]
pub struct ImageProxyConfig {
    /// Path to container auth file; equivalent to `skopeo --authfile`.
    /// This conflicts with [`auth_data`].
    pub authfile: Option<PathBuf>,

    /// Data stream for container auth.  This conflicts with [`authfile`].
    pub auth_data: Option<File>,

    /// Do not use default container authentication paths; equivalent to `skopeo --no-creds`.
    ///
    /// Defaults to `false`; in other words, use the default file paths from `man containers-auth.json`.
    pub auth_anonymous: bool,

    // Directory with certificates (*.crt, *.cert, *.key) used to connect to registry
    // Equivalent to `skopeo --cert-dir`
    pub certificate_directory: Option<PathBuf>,

    /// Decryption keys to decrypt an encrypted container image.
    /// equivalent to `skopeo copy --decryption-key <path_to_decryption_key> `
    pub decryption_keys: Option<Vec<String>>,

    /// If set, disable TLS verification.  Equivalent to `skopeo --tls-verify=false`.
    pub insecure_skip_tls_verification: Option<bool>,

    /// If enabled, propagate debug-logging level from the proxy via stderr to the
    /// current process' stderr. Note than when enabled, this also means that standard
    /// error will no longer be captured.
    pub debug: bool,

    /// Provide a configured [`std::process::Command`] instance.
    ///
    /// This allows configuring aspects of the resulting child `skopeo` process.
    /// The intention of this hook is to allow the caller to use e.g.
    /// `systemd-run` or equivalent containerization tools.  For example you
    /// can set up a command whose arguments are `systemd-run -Pq -p DynamicUser=yes -- skopeo`.
    /// You can also set up arbitrary aspects of the child via e.g.
    /// [`current_dir`] [`pre_exec`].
    ///
    /// [`current_dir`]: https://doc.rust-lang.org/std/process/struct.Command.html#method.current_dir
    /// [`pre_exec`]: https://doc.rust-lang.org/std/os/unix/process/trait.CommandExt.html#tymethod.pre_exec
    ///
    /// The default is to wrap via util-linux `setpriv --pdeathsig SIGTERM -- skopeo`,
    /// which on Linux binds the lifecycle of the child process to the parent.
    ///
    /// Note that you *must* add `skopeo` as the primary argument or
    /// indirectly.  However, all other command line options including
    /// `experimental-image-proxy` will be injected by this library.
    /// You may use a different command name from `skopeo` if your
    /// application has set up a compatible copy, e.g. `/usr/lib/myapp/my-private-skopeo`/
    pub skopeo_cmd: Option<Command>,
}

impl TryFrom<ImageProxyConfig> for Command {
    type Error = Error;

    fn try_from(config: ImageProxyConfig) -> Result<Self> {
        let debug = config.debug || std::env::var_os("CONTAINERS_IMAGE_PROXY_DEBUG").is_some();
        let mut allocated_fds = RESERVED_FD_RANGE.clone();
        let mut alloc_fd = || {
            allocated_fds.next().ok_or_else(|| {
                Error::Other("Ran out of reserved file descriptors for child".into())
            })
        };

        // By default, we set up pdeathsig to "lifecycle bind" the child process to us.
        let mut c = config.skopeo_cmd.unwrap_or_else(|| {
            let mut c = std::process::Command::new("skopeo");
            unsafe {
                c.pre_exec(|| {
                    rustix::process::set_parent_process_death_signal(Some(
                        rustix::process::Signal::TERM,
                    ))
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                });
            }
            c
        });
        c.arg("experimental-image-proxy");
        if debug {
            c.arg("--debug");
        }
        let auth_option_count = [
            config.authfile.is_some(),
            config.auth_data.is_some(),
            config.auth_anonymous,
        ]
        .into_iter()
        .filter(|&x| x)
        .count();
        if auth_option_count > 1 {
            // This is a programmer error really
            return Err(Error::Configuration(
                "Conflicting authentication options".into(),
            ));
        }
        if let Some(authfile) = config.authfile {
            c.arg("--authfile");
            c.arg(authfile);
        } else if let Some(mut auth_data) = config.auth_data.map(std::io::BufReader::new) {
            // If we get the authentication data as a file, we always copy it to a new temporary file under
            // the assumption that the caller provided it this way to aid in privilege separation where
            // the file is only readable to privileged code.
            let target_fd = alloc_fd()?;
            let tmpd = &cap_std::fs::Dir::open_ambient_dir("/tmp", cap_std::ambient_authority())?;
            let mut tempfile =
                cap_tempfile::TempFile::new_anonymous(tmpd).map(std::io::BufWriter::new)?;
            std::io::copy(&mut auth_data, &mut tempfile)?;
            let tempfile = tempfile
                .into_inner()
                .map_err(|e| e.into_error())?
                .into_std();
            let fd = std::sync::Arc::new(tempfile.into());
            c.take_fd_n(fd, target_fd);
            c.arg("--authfile");
            c.arg(format!("/proc/self/fd/{target_fd}"));
        } else if config.auth_anonymous {
            c.arg("--no-creds");
        }

        if let Some(certificate_directory) = config.certificate_directory {
            c.arg("--cert-dir");
            c.arg(certificate_directory);
        }

        if let Some(decryption_keys) = config.decryption_keys {
            for decryption_key in &decryption_keys {
                c.arg("--decryption-key");
                c.arg(decryption_key);
            }
        }

        if config.insecure_skip_tls_verification.unwrap_or_default() {
            c.arg("--tls-verify=false");
        }
        c.stdout(Stdio::null());
        if !debug {
            c.stderr(Stdio::piped());
        }
        Ok(c)
    }
}

/// BlobInfo collects known information about a blob
#[derive(Debug, serde::Deserialize)]
pub struct ConvertedLayerInfo {
    /// Uncompressed digest of a layer; for more information, see
    /// https://github.com/opencontainers/image-spec/blob/main/config.md#layer-diffid
    pub digest: Digest,

    /// Size of blob
    pub size: u64,

    /// Mediatype of blob
    pub media_type: oci_spec::image::MediaType,
}

/// A single fd; requires invoking FinishPipe
#[derive(Debug)]
struct FinishPipe {
    pipeid: PipeId,
    datafd: OwnedFd,
}

/// There is a data FD and an error FD. The error FD will be JSON.
#[derive(Debug)]
struct DualFds {
    datafd: OwnedFd,
    errfd: OwnedFd,
}

/// Helper trait for parsing the pipeid and/or file descriptors of a reply
trait FromReplyFds: Send + 'static
where
    Self: Sized,
{
    fn from_reply(
        iterable: impl IntoIterator<IntoIter: FusedIterator, Item = OwnedFd>,
        pipeid: u32,
    ) -> Result<Self>;
}

/// No file descriptors or pipeid expected
impl FromReplyFds for () {
    fn from_reply(fds: impl IntoIterator<Item = OwnedFd>, pipeid: u32) -> Result<Self> {
        if fds.into_iter().next().is_some() {
            return Err(Error::Other("expected no fds".into()));
        }
        if pipeid != 0 {
            return Err(Error::Other("unexpected pipeid".into()));
        }
        Ok(())
    }
}

/// A FinishPipe instance
impl FromReplyFds for FinishPipe {
    fn from_reply(fds: impl IntoIterator<Item = OwnedFd>, pipeid: u32) -> Result<Self> {
        let Some(pipeid) = PipeId::try_new(pipeid) else {
            return Err(Error::Other("Expected pipeid for FinishPipe".into()));
        };
        let datafd = fds
            .into_iter()
            .exactly_one()
            .map_err(|_| Error::Other("Expected exactly one fd for FinishPipe".into()))?;
        Ok(Self { pipeid, datafd })
    }
}

/// A DualFds instance
impl FromReplyFds for DualFds {
    fn from_reply(fds: impl IntoIterator<Item = OwnedFd>, pipeid: u32) -> Result<Self> {
        if pipeid != 0 {
            return Err(Error::Other("Unexpected pipeid with DualFds".into()));
        }
        let [datafd, errfd] = fds
            .into_iter()
            .collect_array()
            .ok_or_else(|| Error::Other("Expected two fds for DualFds".into()))?;
        Ok(Self { datafd, errfd })
    }
}

impl ImageProxy {
    /// Create an image proxy that fetches the target image, using default configuration.
    pub async fn new() -> Result<Self> {
        Self::new_with_config(Default::default()).await
    }

    /// Create an image proxy that fetches the target image
    #[instrument]
    pub async fn new_with_config(config: ImageProxyConfig) -> Result<Self> {
        let mut c = Command::try_from(config)?;
        let (mysock, theirsock) = rustix::net::socketpair(
            rustix::net::AddressFamily::UNIX,
            rustix::net::SocketType::SEQPACKET,
            rustix::net::SocketFlags::CLOEXEC,
            None,
        )?;
        c.stdin(Stdio::from(theirsock));
        let child = match c.spawn() {
            Ok(c) => c,
            Err(error) => return Err(Error::SkopeoSpawnError(error)),
        };
        tracing::debug!("Spawned skopeo pid={:?}", child.id());
        // Here we use std sync API via thread because tokio installs
        // a SIGCHLD handler which can conflict with e.g. the glib one
        // which may also be in process.
        // xref https://github.com/tokio-rs/tokio/issues/3520#issuecomment-968985861
        let childwait = tokio::task::spawn_blocking(move || child.wait_with_output());
        let sockfd = Arc::new(Mutex::new(mysock));

        let mut r = Self {
            sockfd,
            childwait: Arc::new(AsyncMutex::new(Box::pin(childwait))),
            protover: semver::Version::new(0, 0, 0),
        };

        // Verify semantic version
        let (protover, _): (String, ()) = r.impl_request("Initialize", [(); 0]).await?;
        tracing::debug!("Remote protocol version: {protover}");
        let protover = semver::Version::parse(protover.as_str())?;
        // Previously we had a feature to opt-in to requiring newer versions using `if cfg!()`.
        let supported = base_proto_version();
        if !supported.matches(&protover) {
            return Err(Error::ProxyTooOld {
                requested_version: protover.to_string().into(),
                found_version: supported.to_string().into(),
            });
        }
        r.protover = protover;

        Ok(r)
    }

    async fn impl_request_raw<T: serde::de::DeserializeOwned + Send + 'static, F: FromReplyFds>(
        sockfd: Arc<Mutex<OwnedFd>>,
        req: Request,
    ) -> Result<(T, F)> {
        tracing::trace!("sending request {}", req.method.as_str());
        // TODO: Investigate https://crates.io/crates/uds for SOCK_SEQPACKET tokio
        let r = tokio::task::spawn_blocking(move || {
            let sockfd = sockfd.lock().unwrap();
            let sendbuf = serde_json::to_vec(&req)?;
            let sockfd = &*sockfd;
            rustix::net::send(sockfd, &sendbuf, rustix::net::SendFlags::empty())?;
            drop(sendbuf);
            let mut buf = [0u8; MAX_MSG_SIZE];
            let mut cmsg_space: Vec<std::mem::MaybeUninit<u8>> =
                vec![std::mem::MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
            let mut cmsg_buffer = rustix::net::RecvAncillaryBuffer::new(cmsg_space.as_mut_slice());
            let iov = std::io::IoSliceMut::new(buf.as_mut());
            let mut iov = [iov];
            let nread = rustix::net::recvmsg(
                sockfd,
                &mut iov,
                &mut cmsg_buffer,
                rustix::net::RecvFlags::CMSG_CLOEXEC,
            )?
            .bytes;
            let fdret = cmsg_buffer
                .drain()
                .filter_map(|m| match m {
                    rustix::net::RecvAncillaryMessage::ScmRights(f) => Some(f),
                    _ => None,
                })
                .flatten();
            let buf = &buf[..nread];
            let reply: Reply = serde_json::from_slice(buf)?;
            if !reply.success {
                return Err(Error::RequestInitiationFailure {
                    method: req.method.clone().into(),
                    error: reply.error.into(),
                });
            }
            let fds = FromReplyFds::from_reply(fdret, reply.pipeid)?;
            Ok((serde_json::from_value(reply.value)?, fds))
        })
        .await
        .map_err(|e| Error::Other(e.to_string().into()))??;
        tracing::trace!("completed request");
        Ok(r)
    }

    #[instrument(skip(args))]
    async fn impl_request<T: serde::de::DeserializeOwned + Send + 'static, F: FromReplyFds>(
        &self,
        method: &str,
        args: impl IntoIterator<Item = impl Into<serde_json::Value>>,
    ) -> Result<(T, F)> {
        let req = Self::impl_request_raw(Arc::clone(&self.sockfd), Request::new(method, args));
        let mut childwait = self.childwait.lock().await;
        tokio::select! {
            r = req => { r }
            r = childwait.as_mut() => {
                let r = r.map_err(|e| Error::Other(e.to_string().into()))??;
                let stderr = String::from_utf8_lossy(&r.stderr);
                Err(Error::Other(format!("skopeo proxy unexpectedly exited during request method {}: {}\n{}", method, r.status, stderr).into()))
            }
        }
    }

    #[instrument]
    async fn finish_pipe(&self, pipeid: PipeId) -> Result<()> {
        tracing::debug!("closing pipe");
        let (r, ()) = self.impl_request("FinishPipe", [pipeid.0.get()]).await?;
        Ok(r)
    }

    #[instrument]
    pub async fn open_image(&self, imgref: &str) -> Result<OpenedImage> {
        tracing::debug!("opening image");
        let (imgid, ()) = self.impl_request("OpenImage", [imgref]).await?;
        Ok(OpenedImage(imgid))
    }

    #[instrument]
    pub async fn open_image_optional(&self, imgref: &str) -> Result<Option<OpenedImage>> {
        tracing::debug!("opening image");
        let (imgid, ()) = self.impl_request("OpenImageOptional", [imgref]).await?;
        if imgid == 0 {
            Ok(None)
        } else {
            Ok(Some(OpenedImage(imgid)))
        }
    }

    #[instrument]
    pub async fn close_image(&self, img: &OpenedImage) -> Result<()> {
        tracing::debug!("closing image");
        let (r, ()) = self.impl_request("CloseImage", [img.0]).await?;
        Ok(r)
    }

    async fn read_finish_pipe(&self, pipe: FinishPipe) -> Result<Vec<u8>> {
        let fd = tokio::fs::File::from_std(std::fs::File::from(pipe.datafd));
        let mut fd = tokio::io::BufReader::new(fd);
        let mut r = Vec::new();
        let reader = fd.read_to_end(&mut r);
        let (nbytes, finish) = tokio::join!(reader, self.finish_pipe(pipe.pipeid));
        finish?;
        assert_eq!(nbytes?, r.len());
        Ok(r)
    }

    /// Fetch the manifest as raw bytes, converted to OCI if necessary.
    /// The original digest of the unconverted manifest is also returned.
    /// For more information on OCI manifests, see <https://github.com/opencontainers/image-spec/blob/main/manifest.md>
    pub async fn fetch_manifest_raw_oci(&self, img: &OpenedImage) -> Result<(String, Vec<u8>)> {
        let (digest, pipefd) = self.impl_request("GetManifest", [img.0]).await?;
        Ok((digest, self.read_finish_pipe(pipefd).await?))
    }

    /// Fetch the manifest.
    /// For more information on OCI manifests, see <https://github.com/opencontainers/image-spec/blob/main/manifest.md>
    pub async fn fetch_manifest(
        &self,
        img: &OpenedImage,
    ) -> Result<(String, oci_spec::image::ImageManifest)> {
        let (digest, raw) = self.fetch_manifest_raw_oci(img).await?;
        let manifest = serde_json::from_slice(&raw)?;
        Ok((digest, manifest))
    }

    /// Fetch the config.
    /// For more information on OCI config, see <https://github.com/opencontainers/image-spec/blob/main/config.md>
    pub async fn fetch_config_raw(&self, img: &OpenedImage) -> Result<Vec<u8>> {
        let ((), pipe) = self.impl_request("GetFullConfig", [img.0]).await?;
        self.read_finish_pipe(pipe).await
    }

    /// Fetch the config.
    /// For more information on OCI config, see <https://github.com/opencontainers/image-spec/blob/main/config.md>
    pub async fn fetch_config(
        &self,
        img: &OpenedImage,
    ) -> Result<oci_spec::image::ImageConfiguration> {
        let raw = self.fetch_config_raw(img).await?;
        serde_json::from_slice(&raw).map_err(Into::into)
    }

    /// Fetch a blob identified by e.g. `sha256:<digest>`.
    /// <https://github.com/opencontainers/image-spec/blob/main/descriptor.md>
    ///
    /// The requested size and digest are verified (by the proxy process).
    ///
    /// Note that because of the implementation details of this function, you should
    /// [`futures::join!`] the returned futures instead of polling one after the other. The
    /// secondary "driver" future will only return once everything has been read from
    /// the reader future.
    #[instrument]
    pub async fn get_blob(
        &self,
        img: &OpenedImage,
        digest: &Digest,
        size: u64,
    ) -> Result<(
        impl AsyncBufRead + Send + Unpin,
        impl Future<Output = Result<()>> + Unpin + '_,
    )> {
        // For previous discussion on digest/size verification, see
        // https://github.com/cgwalters/container-image-proxy/issues/1#issuecomment-926712009
        tracing::debug!("fetching blob");
        let args: Vec<serde_json::Value> =
            vec![img.0.into(), digest.to_string().into(), size.into()];
        let (bloblen, pipe): (u64, FinishPipe) = self.impl_request("GetBlob", args).await?;
        let _: u64 = bloblen;
        let fd = tokio::fs::File::from_std(std::fs::File::from(pipe.datafd));
        let fd = tokio::io::BufReader::new(fd);
        let finish = Box::pin(self.finish_pipe(pipe.pipeid));
        Ok((fd, finish))
    }

    async fn read_blob_error(fd: OwnedFd) -> std::result::Result<(), GetBlobError> {
        let fd = tokio::fs::File::from_std(std::fs::File::from(fd));
        let mut errfd = tokio::io::BufReader::new(fd);
        let mut buf = Vec::new();
        errfd
            .read_to_end(&mut buf)
            .await
            .map_err(|e| GetBlobError::Other(e.to_string().into_boxed_str()))?;
        if buf.is_empty() {
            return Ok(());
        }
        #[derive(Deserialize)]
        struct RemoteError {
            code: String,
            message: String,
        }
        let e: RemoteError = serde_json::from_slice(&buf)
            .map_err(|e| GetBlobError::Other(e.to_string().into_boxed_str()))?;
        match e.code.as_str() {
            // Actually this is OK
            "EPIPE" => Ok(()),
            "retryable" => Err(GetBlobError::Retryable(e.message.into_boxed_str())),
            _ => Err(GetBlobError::Other(e.message.into_boxed_str())),
        }
    }

    /// Fetch a blob identified by e.g. `sha256:<digest>`; does not perform
    /// any verification that the blob matches the digest. The size of the
    /// blob and a pipe file descriptor are returned.
    #[instrument]
    pub async fn get_raw_blob(
        &self,
        img: &OpenedImage,
        digest: &Digest,
    ) -> Result<(
        u64,
        tokio::fs::File,
        impl Future<Output = std::result::Result<(), GetBlobError>> + Unpin + '_,
    )> {
        tracing::debug!("fetching blob");
        let args: Vec<serde_json::Value> = vec![img.0.into(), digest.to_string().into()];
        let (bloblen, fds): (u64, DualFds) = self.impl_request("GetRawBlob", args).await?;
        let fd = tokio::fs::File::from_std(std::fs::File::from(fds.datafd));
        let err = Self::read_blob_error(fds.errfd).boxed();
        Ok((bloblen, fd, err))
    }

    /// Fetch a descriptor. The requested size and digest are verified (by the proxy process).
    #[instrument]
    pub async fn get_descriptor(
        &self,
        img: &OpenedImage,
        descriptor: &Descriptor,
    ) -> Result<(
        impl AsyncBufRead + Send + Unpin,
        impl Future<Output = Result<()>> + Unpin + '_,
    )> {
        self.get_blob(img, descriptor.digest(), descriptor.size())
            .await
    }

    ///Returns data that can be used to find the "diffid" corresponding to a particular layer.
    #[instrument]
    pub async fn get_layer_info(
        &self,
        img: &OpenedImage,
    ) -> Result<Option<Vec<ConvertedLayerInfo>>> {
        tracing::debug!("Getting layer info");
        if layer_info_piped_proto_version().matches(&self.protover) {
            let ((), pipe) = self.impl_request("GetLayerInfoPiped", [img.0]).await?;
            let buf = self.read_finish_pipe(pipe).await?;
            return Ok(Some(serde_json::from_slice(&buf)?));
        }
        if !layer_info_proto_version().matches(&self.protover) {
            return Ok(None);
        }
        let (layers, ()) = self.impl_request("GetLayerInfo", [img.0]).await?;
        Ok(Some(layers))
    }

    /// Close the connection and wait for the child process to exit successfully.
    #[instrument]
    pub async fn finalize(self) -> Result<()> {
        let _ = &self;
        let req = Request::new_bare("Shutdown");
        let sendbuf = serde_json::to_vec(&req)?;
        // SAFETY: Only panics if a worker thread already panic'd
        let sockfd = Arc::try_unwrap(self.sockfd).unwrap().into_inner().unwrap();
        rustix::net::send(sockfd, &sendbuf, rustix::net::SendFlags::empty())?;
        drop(sendbuf);
        tracing::debug!("sent shutdown request");
        let mut childwait = self.childwait.lock().await;
        let output = childwait
            .as_mut()
            .await
            .map_err(|e| Error::new_other(e.to_string()))??;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::RequestReturned(
                format!("proxy failed: {}\n{}", output.status, stderr).into(),
            ));
        }
        tracing::debug!("proxy exited successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufWriter, Seek, Write};
    use std::os::fd::{AsRawFd, OwnedFd};

    use super::*;
    use cap_std_ext::cap_std::fs::Dir;
    use rustix::fs::{memfd_create, MemfdFlags};

    fn validate(c: Command, contains: &[&str], not_contains: &[&str]) {
        // Format via debug, because
        // https://doc.rust-lang.org/std/process/struct.Command.html#method.get_args
        // is experimental
        let d = format!("{:?}", c);
        for c in contains {
            assert!(d.contains(c), "{} missing {}", d, c);
        }
        for c in not_contains {
            assert!(!d.contains(c), "{} should not contain {}", d, c);
        }
    }

    #[test]
    fn proxy_configs() {
        let tmpd = &cap_tempfile::tempdir(cap_std::ambient_authority()).unwrap();

        let c = Command::try_from(ImageProxyConfig::default()).unwrap();
        validate(
            c,
            &["experimental-image-proxy"],
            &["--no-creds", "--tls-verify", "--authfile"],
        );

        let c = Command::try_from(ImageProxyConfig {
            authfile: Some(PathBuf::from("/path/to/authfile")),
            ..Default::default()
        })
        .unwrap();
        validate(c, &[r"--authfile", "/path/to/authfile"], &[]);

        let decryption_key_path = "/path/to/decryption_key";
        let c = Command::try_from(ImageProxyConfig {
            decryption_keys: Some(vec![decryption_key_path.to_string()]),
            ..Default::default()
        })
        .unwrap();
        validate(c, &[r"--decryption-key", "/path/to/decryption_key"], &[]);

        let c = Command::try_from(ImageProxyConfig {
            certificate_directory: Some(PathBuf::from("/path/to/certs")),
            ..Default::default()
        })
        .unwrap();
        validate(c, &[r"--cert-dir", "/path/to/certs"], &[]);

        let c = Command::try_from(ImageProxyConfig {
            insecure_skip_tls_verification: Some(true),
            ..Default::default()
        })
        .unwrap();
        validate(c, &[r"--tls-verify=false"], &[]);

        let mut tmpf = cap_tempfile::TempFile::new_anonymous(tmpd).unwrap();
        tmpf.write_all(r#"{ "auths": {} "#.as_bytes()).unwrap();
        tmpf.seek(std::io::SeekFrom::Start(0)).unwrap();
        let c = Command::try_from(ImageProxyConfig {
            auth_data: Some(tmpf.into_std()),
            ..Default::default()
        })
        .unwrap();
        validate(c, &["--authfile", "/proc/self/fd/100"], &[]);
    }

    #[tokio::test]
    async fn skopeo_not_found() {
        let mut config = ImageProxyConfig {
            ..ImageProxyConfig::default()
        };
        config.skopeo_cmd = Some(Command::new("no-skopeo"));

        match ImageProxy::new_with_config(config).await {
            Ok(_) => panic!("Expected an error"),
            Err(ref e @ Error::SkopeoSpawnError(ref inner)) => {
                assert_eq!(inner.kind(), std::io::ErrorKind::NotFound);
                // Just to double check
                assert!(e
                    .to_string()
                    .contains("skopeo spawn error: No such file or directory"));
            }
            Err(e) => panic!("Unexpected error {e}"),
        }
    }

    #[tokio::test]
    async fn test_proxy_send_sync() {
        fn assert_send_sync(_x: impl Send + Sync) {}

        let Ok(proxy) = ImageProxy::new().await else {
            // doesn't matter: we only actually care to test if this compiles
            return;
        };
        assert_send_sync(&proxy);
        assert_send_sync(proxy);

        let opened = OpenedImage(0);
        assert_send_sync(&opened);
        assert_send_sync(opened);
    }

    fn generate_err_fd(v: serde_json::Value) -> Result<OwnedFd> {
        let tmp = Dir::open_ambient_dir("/tmp", cap_std::ambient_authority())?;
        let mut tf = cap_tempfile::TempFile::new_anonymous(&tmp).map(BufWriter::new)?;
        serde_json::to_writer(&mut tf, &v)?;
        let mut tf = tf.into_inner().map_err(|e| e.into_error())?;
        tf.seek(std::io::SeekFrom::Start(0))?;
        let r = tf.into_std().into();
        Ok(r)
    }

    #[tokio::test]
    async fn test_read_blob_error_retryable() -> Result<()> {
        let retryable = serde_json::json!({
            "code": "retryable",
            "message": "foo",
        });
        let retryable = generate_err_fd(retryable)?;
        let err = ImageProxy::read_blob_error(retryable).boxed();
        let e = err.await.unwrap_err();
        match e {
            GetBlobError::Retryable(s) => assert_eq!(s.as_ref(), "foo"),
            _ => panic!("Unexpected error {e:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_read_blob_error_none() -> Result<()> {
        let tmp = Dir::open_ambient_dir("/tmp", cap_std::ambient_authority())?;
        let tf = cap_tempfile::TempFile::new_anonymous(&tmp)?.into_std();
        let err = ImageProxy::read_blob_error(tf.into()).boxed();
        err.await.unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_read_blob_error_other() -> Result<()> {
        let other = serde_json::json!({
            "code": "other",
            "message": "bar",
        });
        let other = generate_err_fd(other)?;
        let err = ImageProxy::read_blob_error(other).boxed();
        let e = err.await.unwrap_err();
        match e {
            GetBlobError::Other(s) => assert_eq!(s.as_ref(), "bar"),
            _ => panic!("Unexpected error {e:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_read_blob_error_epipe() -> Result<()> {
        let epipe = serde_json::json!({
            "code": "EPIPE",
            "message": "baz",
        });
        let epipe = generate_err_fd(epipe)?;
        let err = ImageProxy::read_blob_error(epipe).boxed();
        err.await.unwrap();
        Ok(())
    }

    // Helper to create a dummy OwnedFd using memfd_create for testing.
    fn create_dummy_fd() -> OwnedFd {
        memfd_create(c"test-fd", MemfdFlags::CLOEXEC).unwrap()
    }

    #[test]
    fn test_new_from_raw_values_finish_pipe() {
        let datafd = create_dummy_fd();
        // Keep a raw fd to compare later, as fds_and_pipeid consumes datafd
        let raw_datafd_val = datafd.as_raw_fd();
        let fds = vec![datafd];
        let v = FinishPipe::from_reply(fds, 1).unwrap();
        assert_eq!(v.pipeid.0.get(), 1);
        assert_eq!(v.datafd.as_raw_fd(), raw_datafd_val);
    }

    #[test]
    fn test_new_from_raw_values_dual_fds() {
        let datafd = create_dummy_fd();
        let errfd = create_dummy_fd();
        let raw_datafd_val = datafd.as_raw_fd();
        let raw_errfd_val = errfd.as_raw_fd();
        let fds = vec![datafd, errfd];
        let v = DualFds::from_reply(fds, 0).unwrap();
        assert_eq!(v.datafd.as_raw_fd(), raw_datafd_val);
        assert_eq!(v.errfd.as_raw_fd(), raw_errfd_val);
    }

    #[test]
    fn test_new_from_raw_values_error_too_many_fds() {
        let fds = vec![create_dummy_fd(), create_dummy_fd(), create_dummy_fd()];
        match DualFds::from_reply(fds, 0) {
            Ok(v) => unreachable!("{v:?}"),
            Err(Error::Other(msg)) => {
                assert_eq!(msg.as_ref(), "Expected two fds for DualFds")
            }
            Err(other) => unreachable!("{other}"),
        }
    }

    #[test]
    fn test_new_from_raw_values_error_fd_with_zero_pipeid() {
        let fds = vec![create_dummy_fd()];
        match FinishPipe::from_reply(fds, 0) {
            Ok(v) => unreachable!("{v:?}"),
            Err(Error::Other(msg)) => {
                assert_eq!(msg.as_ref(), "Expected pipeid for FinishPipe")
            }
            Err(other) => unreachable!("{other}"),
        }
    }

    #[test]
    fn test_new_from_raw_values_error_pipeid_with_both_fds() {
        let fds = vec![create_dummy_fd(), create_dummy_fd()];
        match DualFds::from_reply(fds, 1) {
            Ok(v) => unreachable!("{v:?}"),
            Err(Error::Other(msg)) => {
                assert_eq!(msg.as_ref(), "Unexpected pipeid with DualFds")
            }
            Err(other) => unreachable!("{other}"),
        }
    }

    #[test]
    fn test_new_from_raw_values_error_no_fd_with_pipeid() {
        let fds: Vec<OwnedFd> = vec![];
        match FinishPipe::from_reply(fds, 1) {
            Ok(v) => unreachable!("{v:?}"),
            Err(Error::Other(msg)) => {
                assert_eq!(msg.as_ref(), "Expected exactly one fd for FinishPipe")
            }
            Err(other) => unreachable!("{other}"),
        }
    }
}
