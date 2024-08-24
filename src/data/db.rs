use std::ops::AddAssign;
use std::time::Instant;
use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use clickhouse::inserter::{Inserter, Quantities};
use clickhouse::{sql, Client, Row};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
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
    quote_qty: f32,
    /// Was this the best price available on the exchange?
    is_best_match: bool,
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
            quote_qty: row.quote_qty,
            is_best_match: row.is_best_match,
            id: row.id,
        }
    }
}

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
                    pair LowCardinality(String) COMMENT 'Pair being traded BASE ASSET IN DENOM',
                    side Boolean COMMENT 'Long=True; Short=False',
                    price Float32 COMMENT 'Asset price in DENOM',
                    qty Float32 COMMENT 'Trade QTY in BASE ASSET',
                    quote_qty Float32 COMMENT 'price * qty; Notional value',
                    is_best_match Boolean COMMENT 'Was this the best price available',
                    id UInt32 COMMENT 'Trade id',
                )
                ENGINE = MergeTree
                PRIMARY KEY (time, pair)
                ORDER BY (time, pair)
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

        let mut inserter = self
            .client
            .inserter::<CHRow>(&self.name)?
            .with_max_rows(100_000)
            .with_period(Some(Duration::from_secs(15)));

        let mut file_count = 0;
        let mut stats = AddableQuantities::default();
        while let Some(file) = rx.recv().await {
            stats += self.index_file(&mut inserter, file).await?;
            file_count += 1;
        }

        stats += inserter.end().await?;
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

    pub async fn index_file(
        &self,
        inserter: &mut Inserter<CHRow>,
        file: File,
    ) -> Result<AddableQuantities> {
        log::info!(
            "[{}] Indexing pair={}; file={}",
            self.name,
            file.pair,
            file.path.to_string_lossy()
        );

        let mut tx: u16 = 0;
        let now = Instant::now();
        let mut stats = AddableQuantities::default();
        let mut records = file.records().await?;

        // This works, but this is slow because it's a single indexing thread
        while let Some(row) = records.next().await {
            let row = row?;
            inserter.write(&CHRow::new(&file.pair, row))?;
            tx += 1;

            // insert in batches of 1000
            if tx.rem_euclid(1000) == 0 {
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
        log::debug!(
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
