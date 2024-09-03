pub mod data;
pub mod test_utils;
pub mod utils;
pub use crate::data::binance::data_types::{Asset, Cadence, DataType};
pub use crate::data::binance::downloader::Downloader;
pub use crate::data::db::trades::TradesTable;

use std::env;
use std::time::Instant;

use anyhow::Result;
use env_logger::{Builder, Target};

#[tokio::main]
async fn main() -> Result<()> {
    Builder::new()
        .target(Target::Stdout)
        .parse_filters(&env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    // perf start
    let now = Instant::now();

    let downloader = Downloader::new(
        "USDC Downloader",
        Asset::Spot,
        Cadence::Monthly,
        DataType::Trades,
    )?
    .with_pair_ends_with(&["USDC"]);

    let table = TradesTable::new("test", "trades_any_usdc", downloader).await?;
    table.index().await?;

    log::info!("[main] Execution took: {:.2?}", now.elapsed());
    log::info!("test")

    Ok(())
}
