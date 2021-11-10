//! Run skopeo as a subprocess to fetch container images.
//!
//! This allows fetching a container image manifest and layers in a streaming fashion.
//!
//! More information: <https://github.com/containers/skopeo/pull/1476>

use anyhow::{anyhow, Context, Result};
use futures_util::Future;
use nix::sys::socket::{self as nixsocket, ControlMessageOwned};
use nix::sys::uio::IoVec;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::{FromRawFd, RawFd};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufRead, AsyncReadExt};
use tokio::sync::Mutex as AsyncMutex;
use tracing::instrument;

pub const OCI_TYPE_LAYER_GZIP: &str = "application/vnd.oci.image.layer.v1.tar+gzip";
pub const OCI_TYPE_LAYER_TAR: &str = "application/vnd.oci.image.layer.v1.tar";

// This is defined in skopeo; maximum size of JSON we will read/write.
// Note that payload data (non-metadata) should go over a pipe file descriptor.
const MAX_MSG_SIZE: usize = 32 * 1024;

lazy_static::lazy_static! {
    static ref SUPPORTED_PROTO_VERSION: semver::VersionReq = {
        semver::VersionReq::parse("0.2.0").unwrap()
    };
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

type ChildFuture = Pin<Box<dyn Future<Output = std::io::Result<std::process::Output>>>>;

/// Manage a child process proxy to fetch container images.
pub struct ImageProxy {
    sockfd: Arc<Mutex<File>>,
    childwait: Arc<AsyncMutex<ChildFuture>>,
}

impl std::fmt::Debug for ImageProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageProxy").finish()
    }
}

/// Opaque identifier for an image
#[derive(Debug, PartialEq, Eq)]
pub struct OpenedImage(u32);

#[allow(unsafe_code)]
fn new_seqpacket_pair() -> Result<(File, File)> {
    let (mysock, theirsock) = nixsocket::socketpair(
        nixsocket::AddressFamily::Unix,
        nixsocket::SockType::SeqPacket,
        None,
        nixsocket::SockFlag::SOCK_CLOEXEC,
    )?;
    // Convert to owned values
    let mysock = unsafe { std::fs::File::from_raw_fd(mysock) };
    let theirsock = unsafe { std::fs::File::from_raw_fd(theirsock) };
    Ok((mysock, theirsock))
}

#[allow(unsafe_code)]
fn file_from_scm_rights(cmsg: ControlMessageOwned) -> Option<File> {
    if let nixsocket::ControlMessageOwned::ScmRights(fds) = cmsg {
        fds.get(0)
            .map(|&fd| unsafe { std::fs::File::from_raw_fd(fd) })
    } else {
        None
    }
}

/// Configuration for the proxy.
#[derive(Debug, Default)]
pub struct ImageProxyConfig {
    /// Path to container auth file; equivalent to `skopeo --authfile`.
    pub authfile: Option<String>,
}

impl ImageProxy {
    /// Create an image proxy that fetches the target image, using default configuration.
    pub async fn new() -> Result<Self> {
        Self::new_with_config(Default::default()).await
    }

    /// Create an image proxy that fetches the target image
    #[instrument]
    pub async fn new_with_config(config: ImageProxyConfig) -> Result<Self> {
        let (mysock, theirsock) = new_seqpacket_pair()?;
        // By default, we use util-linux's `setpriv` to set up pdeathsig to "lifecycle bind"
        // the child process to us.  In the future we should allow easily configuring
        // e.g. systemd-run as a wrapper, etc.
        let mut c = std::process::Command::new("setpriv");
        c.args(&["--pdeathsig", "SIGTERM", "--", "skopeo"]);
        if let Some(authfile) = config.authfile.as_deref() {
            c.args(&["--authfile", authfile]);
        }
        c.arg("experimental-image-proxy");
        c.stdout(Stdio::null()).stderr(Stdio::piped());
        c.stdin(Stdio::from(theirsock));
        let mut c = tokio::process::Command::from(c);
        c.kill_on_drop(true);
        let child = c.spawn().context("Failed to spawn skopeo")?;
        tracing::debug!("Spawned skopeo pid={:?}", child.id());
        let childwait = Box::pin(child.wait_with_output());

        let sockfd = Arc::new(Mutex::new(mysock));

        let r = Self {
            sockfd,
            childwait: Arc::new(AsyncMutex::new(childwait)),
        };

        // Verify semantic version
        let protover = r.impl_request::<String, _, ()>("Initialize", []).await?.0;
        let protover = semver::Version::parse(protover.as_str())?;
        let supported = &*SUPPORTED_PROTO_VERSION;
        if !supported.matches(&protover) {
            return Err(anyhow!(
                "Unsupported protocol version {} (compatible: {})",
                protover,
                supported
            ));
        }

        Ok(r)
    }

    async fn impl_request_raw<T: serde::de::DeserializeOwned + Send + 'static>(
        sockfd: Arc<Mutex<File>>,
        req: Request,
    ) -> Result<(T, Option<(File, u32)>)> {
        tracing::trace!("sending request {}", req.method.as_str());
        // TODO: Investigate https://crates.io/crates/uds for SOCK_SEQPACKET tokio
        let r = tokio::task::spawn_blocking(move || {
            let sockfd = sockfd.lock().unwrap();
            let sendbuf = serde_json::to_vec(&req)?;
            nixsocket::send(sockfd.as_raw_fd(), &sendbuf, nixsocket::MsgFlags::empty())?;
            drop(sendbuf);
            let mut buf = [0u8; MAX_MSG_SIZE];
            let mut cmsg_buffer = nix::cmsg_space!([RawFd; 1]);
            let iov = IoVec::from_mut_slice(buf.as_mut());
            let r = nixsocket::recvmsg(
                sockfd.as_raw_fd(),
                &[iov],
                Some(&mut cmsg_buffer),
                nixsocket::MsgFlags::MSG_CMSG_CLOEXEC,
            )?;
            let buf = &buf[0..r.bytes];
            let mut fdret: Option<File> = None;
            for cmsg in r.cmsgs() {
                if let Some(f) = file_from_scm_rights(cmsg) {
                    fdret = Some(f);
                    break;
                }
            }
            let reply: Reply = serde_json::from_slice(buf).context("Deserializing reply")?;
            if !reply.success {
                return Err(anyhow!("remote error: {}", reply.error));
            }
            let fdret = match (fdret, reply.pipeid) {
                (Some(fd), n) => {
                    if n == 0 {
                        return Err(anyhow!("got fd but no pipeid"));
                    }
                    Some((fd, n))
                }
                (None, n) => {
                    if n != 0 {
                        return Err(anyhow!("got no fd with pipeid {}", n));
                    }
                    None
                }
            };
            let reply = serde_json::from_value(reply.value).context("Deserializing value")?;
            Ok((reply, fdret))
        })
        .await??;
        tracing::trace!("completed request");
        Ok(r)
    }

    #[instrument(skip(args))]
    async fn impl_request<R: serde::de::DeserializeOwned + Send + 'static, T, I>(
        &self,
        method: &str,
        args: T,
    ) -> Result<(R, Option<(File, u32)>)>
    where
        T: IntoIterator<Item = I>,
        I: Into<serde_json::Value>,
    {
        let req = Self::impl_request_raw(Arc::clone(&self.sockfd), Request::new(method, args));
        let mut childwait = self.childwait.lock().await;
        tokio::select! {
            r = req => {
                Ok(r?)
            }
            r = childwait.as_mut() => {
                let r = r?;
                let stderr = String::from_utf8_lossy(&r.stderr);
                return Err(anyhow::anyhow!("proxy unexpectedly exited during request method {}: {}\n{}", method, r.status, stderr))
            }
        }
    }

    #[instrument]
    async fn finish_pipe(&self, pipeid: u32) -> Result<()> {
        tracing::debug!("closing pipe");
        let (r, fd) = self.impl_request("FinishPipe", [pipeid]).await?;
        if fd.is_some() {
            return Err(anyhow!("Unexpected fd in finish_pipe reply"));
        }
        Ok(r)
    }

    #[instrument]
    pub async fn open_image(&self, imgref: &str) -> Result<OpenedImage> {
        tracing::debug!("opening image");
        let (imgid, _) = self
            .impl_request::<u32, _, _>("OpenImage", [imgref])
            .await?;
        Ok(OpenedImage(imgid))
    }

    #[instrument]
    pub async fn close_image(&self, img: &OpenedImage) -> Result<()> {
        tracing::debug!("closing image");
        let (r, _) = self.impl_request("CloseImage", [img.0]).await?;
        Ok(r)
    }

    /// Fetch the manifest.
    /// For more information on OCI manifests, see <https://github.com/opencontainers/image-spec/blob/main/manifest.md>
    pub async fn fetch_manifest(&self, img: &OpenedImage) -> Result<(String, Vec<u8>)> {
        let (digest, fd) = self.impl_request("GetManifest", [img.0]).await?;
        let (fd, pipeid) = fd.ok_or_else(|| anyhow!("Missing fd from reply"))?;
        let mut fd = tokio::io::BufReader::new(tokio::fs::File::from_std(fd));
        let mut manifest = Vec::new();
        let reader = fd.read_to_end(&mut manifest);
        let (reader, finish) = tokio::join!(reader, self.finish_pipe(pipeid));
        reader?;
        finish?;
        Ok((digest, manifest))
    }

    /// Fetch a blob identified by e.g. `sha256:<digest>`.
    /// <https://github.com/opencontainers/image-spec/blob/main/descriptor.md>
    ///
    /// The requested size and digest are verified (by the proxy process).
    #[instrument]
    pub async fn get_blob(
        &self,
        img: &OpenedImage,
        digest: &str,
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
        let (_bloblen, fd) = self.impl_request::<i64, _, _>("GetBlob", args).await?;
        let (fd, pipeid) = fd.ok_or_else(|| anyhow!("Missing fd from reply"))?;
        let fd = tokio::io::BufReader::new(tokio::fs::File::from_std(fd));
        let finish = Box::pin(self.finish_pipe(pipeid));
        Ok((fd, finish))
    }

    /// Close the connection and wait for the child process to exit successfully.
    #[instrument]
    pub async fn finalize(self) -> Result<()> {
        let req = Request::new_bare("Shutdown");
        let sendbuf = serde_json::to_vec(&req)?;
        // SAFETY: Only panics if a worker thread already panic'd
        let sockfd = Arc::try_unwrap(self.sockfd).unwrap().into_inner().unwrap();
        nixsocket::send(sockfd.as_raw_fd(), &sendbuf, nixsocket::MsgFlags::empty())?;
        drop(sendbuf);
        tracing::debug!("sent shutdown request");
        let mut childwait = self.childwait.lock().await;
        let output = childwait.as_mut().await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("proxy failed: {}\n{}", output.status, stderr)
        }
        tracing::debug!("proxy exited successfully");
        Ok(())
    }
}
