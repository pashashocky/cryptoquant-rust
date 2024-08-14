pub mod data;
pub mod utils;
pub use crate::data::binance::data_types::{Asset, Cadence, DataType};
pub use crate::data::binance::downloader::Downloader;

use std::env;

use anyhow::Result;
use env_logger::{Builder, Target};

#[tokio::main]
async fn main() -> Result<()> {
    Builder::new()
        .target(Target::Stdout)
        .parse_filters(&env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let mut bh = Downloader::new(
        "USDC Downloader".into(),
        Asset::Spot,
        Cadence::Monthly,
        DataType::Trades,
    )?
    .with_pair_ends_with(&["USDC"]);
    bh.download().await?;

    Ok(())
}
