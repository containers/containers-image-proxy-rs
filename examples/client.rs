use std::io::Write;

use anyhow::Result;
use clap::Parser;
use oci_spec::image::{Digest, ImageManifest};
use tokio::io::AsyncReadExt;

#[derive(clap::Parser, Debug)]
struct GetMetadataOpts {
    /// The skopeo-style transport:image reference
    reference: String,
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

/// Simple program to greet a person
#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
enum Opt {
    GetMetadata(GetMetadataOpts),
    GetBlob(GetBlobOpts),
}

#[derive(serde::Serialize, Debug)]
struct Metadata {
    digest: String,
    manifest: ImageManifest,
}

async fn get_metadata(o: GetMetadataOpts) -> Result<()> {
    let proxy = containers_image_proxy::ImageProxy::new().await?;
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

async fn run() -> Result<()> {
    match Opt::parse() {
        Opt::GetMetadata(o) => get_metadata(o).await,
        Opt::GetBlob(o) => get_blob(o).await,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{:#}", e);
    }
}
