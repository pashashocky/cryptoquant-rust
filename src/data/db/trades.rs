use std::cmp;
use std::ops::Deref;
use std::time::Instant;
use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use chrono::prelude::*;
use clickhouse::{sql, Client, Row};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use super::utils::create_client;
use super::utils::AddableQuantities;
use crate::data::binance::file::File;
use crate::data::db::trades_index_log::{FileIndexLogRow, TradesIndexLogTable};
use crate::{data::binance::file::Row as FileRow, Downloader};

#[derive(Clone)]
pub struct TradesTable {
    client: Client,
    database: Arc<str>,
    name: Arc<str>,
    downloader: Arc<Downloader>,
}

// TODO: We likely want to wrap this functionality into a trait
// but traits cannot define async functions, which makes this complicated?
// ==> use async_traits crate
impl TradesTable {
    pub async fn new(database: &str, name: &str, downloader: Downloader) -> Result<Self> {
        Ok(TradesTable {
            client: create_client(database).await?,
            database: Arc::from(database),
            name: name.to_ascii_uppercase().into(),
            downloader: Arc::new(downloader),
        })
    }

    pub async fn create(&self) -> Result<()> {
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?
                (
                    dt DateTime64(3, 'UTC') COMMENT 'Trade datetime (dt) in ms',
                    id UInt32 COMMENT 'Trade id',
                    pair LowCardinality(String) COMMENT 'Pair being traded BASE ASSET IN DENOM',
                    side Boolean COMMENT 'Long=True; Short=False',
                    price Float32 COMMENT 'Asset price in DENOM',
                    qty Float32 COMMENT 'Trade QTY in BASE ASSET',
                    notional Float32 COMMENT 'price * qty; Notional value',
                )
                -- Deduplicates rows by key
                ENGINE = ReplacingMergeTree
                
                -- There are duplicates on (dt, pair) because multiple tx's can happen
                -- at the same datetime, so we need id to ensure we don't miss rows.
                PRIMARY KEY (dt, id, pair)
                ORDER BY (dt, id, pair)
            ",
            )
            .bind(sql::Identifier(&self.name))
            .execute()
            .await
            .map_err(|e| anyhow!("Could not create table: {}", e))
    }

    pub async fn index(&self) -> Result<()> {
        // TODO: Db initialization procedure otw this will get called multiple times
        self.create().await?;

        let downloader = Arc::clone(&self.downloader);
        let pairs = downloader.get_pairs().await?;
        let files = downloader.get_files(&pairs).await?;
        let files_stream = files.download_stream(50);

        let self_clone = Arc::new(self.clone());
        let stats = files_stream
            .map(|file_result| {
                let self_clone = Arc::clone(&self_clone);
                tokio::spawn(async move {
                    match file_result {
                        Ok(file) => self_clone.index_file(file).await,
                        Err(_) => Ok(AddableQuantities::default()),
                    }
                })
            })
            .buffer_unordered(10) // Process up to 10 tasks concurrently
            .filter_map(|r| async { r.ok() })
            .fold(AddableQuantities::default(), |mut acc, r| async move {
                if let Ok(quantities) = r {
                    acc += quantities;
                }
                acc
            })
            .await;

        if stats.rows > 0 {
            log::info!(
                "[{}] Inserter summary: {} files, {} bytes, {} rows, {} transactions inserted",
                self.name,
                files.len(), // TODO: count is incorrect here as some files could have failed
                stats.bytes,
                stats.rows,
                stats.transactions,
            );
        }
        Ok(())
    }

    pub async fn index_file(&self, file: File) -> Result<AddableQuantities> {
        // TODO: refactor
        log::info!(
            "[{}] Indexing pair={}; file={}",
            self.name,
            file.pair,
            file.path.to_string_lossy()
        );

        // TODO: don't think we need inserter here -> it would be OK to use the regular
        // `client.insert("table_name")` inserter
        // https://github.com/ClickHouse/clickhouse-rs/tree/main?tab=readme-ov-file#insert-a-batch
        let mut inserter = self
            .client
            .inserter::<TradesRow>(&self.name)?
            .with_max_rows(500_000) // TODO: configurable int
            .with_period(Some(Duration::from_secs(15)));

        let mut tx: u16 = 0;
        let now = Instant::now();
        let mut stats = AddableQuantities::default();
        let mut records = file.records().await?;

        let mut start_id: u32 = u32::MAX;
        let mut end_id: u32 = 0;
        let mut start_dt: u64 = u64::MAX;
        let mut end_dt: u64 = 0;

        while let Some(row) = records.next().await {
            let row = row?;
            start_id = cmp::min(start_id, row.id);
            end_id = cmp::max(end_id, row.id);
            start_dt = cmp::min(start_dt, row.time);
            end_dt = cmp::max(end_dt, row.time);
            inserter.write(&TradesRow::new(&file.pair, row))?;
            tx += 1;

            // insert in batches of 8192 -> capsule size
            // TODO: configurable int
            if tx.rem_euclid(8192) == 0 {
                let local_stats = inserter.commit().await?;
                if local_stats.rows > 0 {
                    log::debug!(
                        "[{}] [Commit] {} bytes, {} rows, {} transactions have been inserted",
                        self.name,
                        local_stats.bytes,
                        local_stats.rows,
                        local_stats.transactions,
                    );
                }
                stats += local_stats;
                tx = 0;
            }
        }
        stats += inserter.end().await?; // close the commit
        log::info!(
            "[{}] Indexed in: {:.2?}; pair={}; file={}",
            self.name,
            now.elapsed(),
            file.pair,
            file.path.to_string_lossy()
        );

        let index_log = TradesIndexLogTable::new(&self.database).await?;
        index_log
            .index_row(FileIndexLogRow {
                filename: file
                    .path
                    .deref()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into(),
                start_id,
                end_id,
                start_period_dt: start_dt,
                end_period_dt: end_dt,
                database: self.database.to_string(),
                table: self.name.to_string(),
                num_rows: stats.rows as u32,
                index_dt: Utc::now().timestamp_millis() as u64,
            })
            .await?;

        Ok(stats)
    }

    pub async fn verify(&self) -> Result<()> {
        // Should verify the table has valid data
        // at the very least,
        // the count of number of rows is equal
        // to the sum of the highest pair (id + 1) for each pair (account for zero idx)
        todo!("Implement verification");
    }
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct TradesRow {
    /// Trade time in unix epoch to ms
    dt: u64,
    /// Name of the pair traded
    // Owned String is faster here than lifetime bound
    pair: String,
    /// Long=true; Short=False
    side: bool,
    /// Execution price in DENOM
    price: f32,
    /// Trade quantity in BASE
    qty: f32,
    /// Notional value; price * qty
    notional: f32,
    /// Trade id
    id: u32,
}

impl TradesRow {
    fn new(pair: &str, row: FileRow) -> Self {
        TradesRow {
            dt: row.time,
            pair: pair.to_owned(),
            side: !row.is_buyer_maker,
            price: row.price,
            qty: row.qty,
            notional: row.quote_qty,
            id: row.id,
        }
    }
}
