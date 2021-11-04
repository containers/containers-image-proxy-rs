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
    childwait: ChildFuture,
}

impl std::fmt::Debug for ImageProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageProxy").finish()
    }
}

/// Opaque identifier for an image
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

impl ImageProxy {
    /// Create an image proxy that fetches the target image.
    pub async fn new() -> Result<Self> {
        let (mysock, theirsock) = new_seqpacket_pair()?;
        let mut c = std::process::Command::new("skopeo");
        c.args(&["experimental-image-proxy"]);
        c.stdout(Stdio::null()).stderr(Stdio::piped());
        c.stdin(Stdio::from(theirsock));
        let mut c = tokio::process::Command::from(c);
        c.kill_on_drop(true);
        let child = c.spawn().context("Failed to spawn skopeo")?;
        let mut childwait = Box::pin(child.wait_with_output());

        let sockfd = Arc::new(Mutex::new(mysock));

        // Verify semantic version
        let protoreq =
            Self::impl_request_raw::<String>(Arc::clone(&sockfd), Request::new_bare("Initialize"));
        let protover = tokio::select! {
            r = protoreq => {
                r?.0
            }
            r = &mut childwait => {
                let r = r?;
                let stderr = String::from_utf8_lossy(&r.stderr);
                return Err(anyhow!("skopeo exited unexpectedly (no support for `experimental-image-proxy`?): {}\n{}", r.status, stderr));
            }
        };
        let protover = semver::Version::parse(protover.as_str())?;
        let supported = &*SUPPORTED_PROTO_VERSION;
        if !supported.matches(&protover) {
            return Err(anyhow!(
                "Unsupported protocol version {} (compatible: {})",
                protover,
                supported
            ));
        }

        let r = Self { sockfd, childwait };
        Ok(r)
    }

    async fn impl_request_raw<T: serde::de::DeserializeOwned + Send + 'static>(
        sockfd: Arc<Mutex<File>>,
        req: Request,
    ) -> Result<(T, Option<(File, u32)>)> {
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
        Ok(r)
    }

    async fn impl_request<R: serde::de::DeserializeOwned + Send + 'static, T, I>(
        &mut self,
        method: &str,
        args: T,
    ) -> Result<(R, Option<(File, u32)>)>
    where
        T: IntoIterator<Item = I>,
        I: Into<serde_json::Value>,
    {
        let req = Self::impl_request_raw(Arc::clone(&self.sockfd), Request::new(method, args));
        tokio::select! {
            r = req => {
                Ok(r?)
            }
            r = &mut self.childwait => {
                let r = r?;
                let stderr = String::from_utf8_lossy(&r.stderr);
                return Err(anyhow::anyhow!("proxy unexpectedly exited during request method {}: {}\n{}", method, r.status, stderr))
            }
        }
    }

    async fn finish_pipe(&mut self, pipeid: u32) -> Result<()> {
        let (r, fd) = self.impl_request("FinishPipe", [pipeid]).await?;
        if fd.is_some() {
            return Err(anyhow!("Unexpected fd in finish_pipe reply"));
        }
        Ok(r)
    }

    pub async fn open_image(&mut self, imgref: &str) -> Result<OpenedImage> {
        let (imgid, _) = self
            .impl_request::<u32, _, _>("OpenImage", [imgref])
            .await?;
        Ok(OpenedImage(imgid))
    }

    pub async fn close_image(&mut self, img: &OpenedImage) -> Result<()> {
        let (r, _) = self.impl_request("CloseImage", [img.0]).await?;
        Ok(r)
    }

    /// Fetch the manifest.
    /// https://github.com/opencontainers/image-spec/blob/main/manifest.md
    pub async fn fetch_manifest(&mut self, img: &OpenedImage) -> Result<(String, Vec<u8>)> {
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
    /// https://github.com/opencontainers/image-spec/blob/main/descriptor.md
    /// Note that right now the proxy does verification of the digest:
    /// https://github.com/cgwalters/container-image-proxy/issues/1#issuecomment-926712009
    pub async fn get_blob(
        &mut self,
        img: &OpenedImage,
        digest: &str,
        size: u64,
    ) -> Result<(
        impl AsyncBufRead + Send + Unpin,
        impl Future<Output = Result<()>> + Unpin + '_,
    )> {
        let args: Vec<serde_json::Value> =
            vec![img.0.into(), digest.to_string().into(), size.into()];
        let (_bloblen, fd) = self.impl_request::<i64, _, _>("GetBlob", args).await?;
        let (fd, pipeid) = fd.ok_or_else(|| anyhow!("Missing fd from reply"))?;
        let fd = tokio::io::BufReader::new(tokio::fs::File::from_std(fd));
        let finish = Box::pin(self.finish_pipe(pipeid));
        Ok((fd, finish))
    }

    /// Close the connection and wait for the child process to exit successfully.
    pub async fn finalize(self) -> Result<()> {
        let req = Request::new_bare("Shutdown");
        let sendbuf = serde_json::to_vec(&req)?;
        // SAFETY: Only panics if a worker thread already panic'd
        let sockfd = Arc::try_unwrap(self.sockfd).unwrap().into_inner().unwrap();
        nixsocket::send(sockfd.as_raw_fd(), &sendbuf, nixsocket::MsgFlags::empty())?;
        drop(sendbuf);
        let output = self.childwait.await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("proxy failed: {}\n{}", output.status, stderr)
        }
        Ok(())
    }
}
