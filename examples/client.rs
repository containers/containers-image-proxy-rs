use std::io::Write;

use anyhow::Result;
use clap::Parser;
use containers_image_proxy::ImageProxyConfig;
use oci_spec::image::{Digest, ImageManifest};
use tokio::io::AsyncReadExt;

#[derive(clap::Parser, Debug)]
struct GetMetadataOpts {
    /// The skopeo-style transport:image reference
    reference: String,

    /// Disable TLS verification
    #[clap(long)]
    insecure: bool,
}

#[derive(clap::Parser, Debug)]
struct GetBlobOpts {
    /// The skopeo-style transport:image reference
    reference: String,

    /// The digest of the target blob to fetch
    digest: Digest,

    /// The size of the blob to fetch
    size: u64,
}

#[derive(clap::Parser, Debug)]
struct FetchContainerToDevNullOpts {
    #[clap(flatten)]
    metaopts: GetMetadataOpts,

    /// Use the "raw" path for fetching blobs
    #[clap(long)]
    raw_blobs: bool,
}

/// Simple program to greet a person
#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
enum Opt {
    GetMetadata(GetMetadataOpts),
    GetBlob(GetBlobOpts),
    GetBlobRaw(GetBlobOpts),
    FetchContainerToDevNull(FetchContainerToDevNullOpts),
}

#[derive(serde::Serialize, Debug)]
struct Metadata {
    digest: String,
    manifest: ImageManifest,
}

impl GetMetadataOpts {
    fn proxy_opts(&self) -> containers_image_proxy::ImageProxyConfig {
        let mut r = ImageProxyConfig::default();
        if self.insecure {
            r.insecure_skip_tls_verification = Some(true)
        }
        r
    }
}

async fn get_metadata(o: GetMetadataOpts) -> Result<()> {
    let config = o.proxy_opts();
    let proxy = containers_image_proxy::ImageProxy::new_with_config(config).await?;
    let img = proxy.open_image(&o.reference).await?;
    let (digest, manifest) = proxy.fetch_manifest(&img).await?;
    let metadata = Metadata { digest, manifest };
    serde_json::to_writer_pretty(&mut std::io::stdout().lock(), &metadata)?;
    Ok(())
}

async fn get_blob(o: GetBlobOpts) -> Result<()> {
    let proxy = containers_image_proxy::ImageProxy::new().await?;
    let img = proxy.open_image(&o.reference).await?;
    let (mut blob, driver) = proxy.get_blob(&img, &o.digest, o.size).await?;

    let mut stdout = std::io::stdout().lock();
    let reader = async move {
        let mut buffer = [0u8; 8192];
        loop {
            let n = blob.read(&mut buffer).await?;
            if n == 0 {
                return anyhow::Ok(());
            }
            stdout.write_all(&buffer[..n])?;
        }
    };

    let (a, b) = tokio::join!(reader, driver);
    a?;
    b?;
    Ok(())
}

async fn get_blob_raw(o: GetBlobOpts) -> Result<()> {
    let proxy = containers_image_proxy::ImageProxy::new().await?;
    let img = proxy.open_image(&o.reference).await?;
    let (_, mut datafd, err) = proxy.get_raw_blob(&img, &o.digest).await?;

    let mut stdout = std::io::stdout().lock();
    let reader = async move {
        let mut buffer = [0u8; 8192];
        loop {
            let n = datafd.read(&mut buffer).await?;
            if n == 0 {
                return anyhow::Ok(());
            }
            stdout.write_all(&buffer[..n])?;
        }
    };

    let (a, b) = tokio::join!(reader, err);
    a?;
    b?;
    Ok(())
}

async fn fetch_container_to_devnull(o: FetchContainerToDevNullOpts) -> Result<()> {
    let config = o.metaopts.proxy_opts();
    let proxy = containers_image_proxy::ImageProxy::new_with_config(config).await?;
    let img = &proxy.open_image(&o.metaopts.reference).await?;
    let manifest = proxy.fetch_manifest(img).await?.1;
    for layer in manifest.layers() {
        let mut devnull = tokio::io::sink();
        if o.raw_blobs {
            let (_, mut blob, err) = proxy.get_raw_blob(img, layer.digest()).await?;
            let copier = tokio::io::copy(&mut blob, &mut devnull);
            let (copier, err) = tokio::join!(copier, err);
            copier?;
            err?;
        } else {
            let (mut blob, driver) = proxy.get_descriptor(img, layer).await?;
            let copier = tokio::io::copy(&mut blob, &mut devnull);
            let (copier, driver) = tokio::join!(copier, driver);
            copier?;
            driver?;
        }
    }
    Ok(())
}

async fn run() -> Result<()> {
    match Opt::parse() {
        Opt::GetMetadata(o) => get_metadata(o).await,
        Opt::GetBlob(o) => get_blob(o).await,
        Opt::GetBlobRaw(o) => get_blob_raw(o).await,
        Opt::FetchContainerToDevNull(o) => fetch_container_to_devnull(o).await,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{:#}", e);
    }
}
