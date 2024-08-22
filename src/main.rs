pub mod data;
pub mod utils;
pub use crate::data::binance::data_types::{Asset, Cadence, DataType};
pub use crate::data::binance::downloader::Downloader;

use std::env;
use std::time::Instant;

use anyhow::Result;
use env_logger::{Builder, Target};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    Builder::new()
        .target(Target::Stdout)
        .parse_filters(&env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let now = Instant::now();
    let mut bh = Downloader::new(
        "USDC Downloader",
        Asset::Spot,
        Cadence::Monthly,
        DataType::Trades,
    )?
    .with_pair_ends_with(&["USDC"]);

    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        bh.download_with_channel(Some(tx)).await?;
        Ok::<_, anyhow::Error>(())
    });

    let mut i = 0;
    while let Some(path) = rx.recv().await {
        log::debug!("Received path={}", path.to_string_lossy());
        i += 1;
    }

    let elapsed = now.elapsed();

    log::info!("Took: {:.2?}", elapsed);
    log::info!("Received {}", i);
    Ok(())
}
