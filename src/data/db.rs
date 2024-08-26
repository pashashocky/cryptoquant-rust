use std::ops::AddAssign;
use std::time::Instant;
use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use clickhouse::inserter::Quantities;
use clickhouse::{sql, Client, Row};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::StreamExt;

use crate::{data::binance::file::Row as FileRow, utils::config, Downloader};

use super::binance::file::File;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AddableQuantities {
    /// The number of uncompressed bytes.
    pub bytes: u64,
    /// The number for rows (calls of [`Inserter::write`]).
    pub rows: u64,
    /// The number of nonempty transactions (calls of [`Inserter::commit`]).
    pub transactions: u64,
}

impl AddAssign<Quantities> for AddableQuantities {
    fn add_assign(&mut self, rhs: Quantities) {
        self.bytes += rhs.bytes;
        self.rows += rhs.rows;
        self.transactions += rhs.transactions;
    }
}

impl AddAssign for AddableQuantities {
    fn add_assign(&mut self, rhs: Self) {
        self.bytes += rhs.bytes;
        self.rows += rhs.rows;
        self.transactions += rhs.transactions;
    }
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct CHRow {
    /// Trade time in unix epoch to ms
    time: u64,
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

impl CHRow {
    fn new(pair: &str, row: FileRow) -> Self {
        CHRow {
            time: row.time,
            pair: pair.to_owned(),
            side: !row.is_buyer_maker,
            price: row.price,
            qty: row.qty,
            notional: row.quote_qty,
            id: row.id,
        }
    }
}

#[derive(Clone)]
pub struct Table {
    client: Client,
    name: Arc<str>,
    downloader: Arc<Downloader>,
}

impl Table {
    pub async fn new(database: &str, name: &str, downloader: Downloader) -> Result<Self> {
        let cfg = config::Config::create().clickhouse;
        let mut client = Client::default()
            .with_url(cfg.url)
            .with_user(cfg.user)
            .with_password(cfg.password);

        let database = &database.to_uppercase();

        client = match create_database(&client, database).await {
            Ok(_) => client.with_database(database),
            Err(e) => return Err(e),
        };

        Ok(Table {
            client,
            name: name.to_ascii_uppercase().into(),
            downloader: Arc::new(downloader),
        })
    }

    pub async fn create(&self) -> Result<()> {
        // TODO: Remove
        self.client
            .query("DROP TABLE IF EXISTS ?")
            .bind(sql::Identifier(&self.name))
            .execute()
            .await?;
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?
                (
                    time DateTime64(3, 'UTC') COMMENT 'Trade time in ms',
                    id UInt32 COMMENT 'Trade id',
                    pair LowCardinality(String) COMMENT 'Pair being traded BASE ASSET IN DENOM',
                    side Boolean COMMENT 'Long=True; Short=False',
                    price Float32 COMMENT 'Asset price in DENOM',
                    qty Float32 COMMENT 'Trade QTY in BASE ASSET',
                    notional Float32 COMMENT 'price * qty; Notional value',
                )
                -- Deduplicates rows by key
                ENGINE = ReplacingMergeTree
                
                -- There are duplicates on (time, pair) because multiple tx's can happen
                -- at the same time, so we need id to ensure we don't miss rows.
                PRIMARY KEY (time, id, pair)
                ORDER BY (time, id, pair)
            ",
            )
            .bind(sql::Identifier(&self.name))
            .execute()
            .await
            .map_err(|e| anyhow!("Could not create table: {}", e))
    }

    pub async fn index(&self) -> Result<()> {
        // make sure table exists
        self.create().await?;

        let (tx, mut rx) = mpsc::channel(10);
        let downloader = Arc::clone(&self.downloader);

        // fetch files
        tokio::spawn(async move {
            let pairs = downloader.get_pairs().await?;
            let files = downloader.get_files(&pairs).await?;
            files.download_with_channel(Some(tx)).await?;
            Ok::<_, anyhow::Error>(())
        });

        let mut handles = Vec::new();
        let mut stats = AddableQuantities::default();
        let self_clone = Arc::new(self.clone());
        // TODO: make configurable
        let semaphore = Arc::new(Semaphore::new(10)); // 10 parallel tasks

        // Threaded tasks
        while let Some(file) = rx.recv().await {
            let permit = semaphore.clone().acquire_owned().await?;
            let self_clone = Arc::clone(&self_clone);

            let handle = tokio::spawn(async move {
                let stats = self_clone.index_file(file).await?;
                drop(permit);
                Ok::<_, anyhow::Error>(stats)
            });
            handles.push(handle);
        }
        let mut file_count = 0;
        for handle in handles {
            stats += handle.await??;
            file_count += 1;
        }

        if stats.rows > 0 {
            log::info!(
                "[{}] Inserter summary: {} files, {} bytes, {} rows, {} transactions have been inserted",
                self.name,
                file_count,
                stats.bytes,
                stats.rows,
                stats.transactions,
            );
        }
        Ok(())
    }

    pub async fn index_file(&self, file: File) -> Result<AddableQuantities> {
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
            .inserter::<CHRow>(&self.name)?
            .with_max_rows(500_000) // TODO: configurable int
            .with_period(Some(Duration::from_secs(15)));

        let mut tx: u16 = 0;
        let now = Instant::now();
        let mut stats = AddableQuantities::default();
        let mut records = file.records().await?;

        while let Some(row) = records.next().await {
            let row = row?;
            inserter.write(&CHRow::new(&file.pair, row))?;
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
        Ok(stats)
    }
}

async fn create_database(client: &Client, database: &str) -> Result<()> {
    client
        .query("CREATE DATABASE IF NOT EXISTS ?")
        .bind(sql::Identifier(database))
        .execute()
        .await
        .with_context(|| format!("Could not create database: {}", database))
}
