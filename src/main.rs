pub mod data;
pub mod utils;
pub use crate::data::binance::binance_history::BinanceHistory;
pub use crate::data::binance::data_types::{Asset, Cadence, DataType};

use std::env;

use anyhow::Result;
use env_logger::{Builder, Target};

#[tokio::main]
async fn main() -> Result<()> {
    Builder::new()
        .target(Target::Stdout)
        .parse_filters(&env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let mut bh = BinanceHistory::new(Asset::Spot, Cadence::Monthly, DataType::Trades, "BTCUSDC")?;
    bh.get_files().await?;
    bh.download().await?;

    Ok(())
}
