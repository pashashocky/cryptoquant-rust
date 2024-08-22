pub mod data;
pub mod utils;
pub use crate::data::binance::data_types::{Asset, Cadence, DataType};
pub use crate::data::binance::downloader::Downloader;
// pub use crate::data::binance::file::Row;
pub use crate::data::db::Table;
// use crate::utils::config;

use std::env;
// use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
// use anyhow::Context;
// use async_zip::base::read::seek::ZipFileReader;
use env_logger::{Builder, Target};
// use tokio::fs::File;
// use tokio::io::BufReader;
// use tokio_stream::StreamExt;
// use tokio_util::compat::FuturesAsyncReadCompatExt;

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
    .with_pair_ends_with(&["ZKUSDC"]);

    let table = Table::new("test", "trades_any_usdc", downloader).await?;
    table.index().await?;

    log::info!("Took: {:.2?}", now.elapsed());

    Ok(())
}
