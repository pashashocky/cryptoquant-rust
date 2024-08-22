use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use async_zip::base::read::seek::ZipFileReader;
use clickhouse::{sql, Client, Row};
use serde::{Deserialize, Serialize};
use tokio::{fs::File, io::BufReader, sync::mpsc};
use tokio_stream::StreamExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::{data::binance::file::Row as FileRow, utils::config, Downloader};

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct CHRow<'a> {
    /// Trade time in unix epoch to ms
    time: u64,
    /// Name of the pair traded
    pair: &'a str,
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

impl<'a> CHRow<'a> {
    fn new(pair: &'a str, row: FileRow) -> Self {
        CHRow {
            time: row.time,
            pair,
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
    downloader: Downloader,
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
            downloader,
        })
    }

    pub async fn create(&self) -> Result<()> {
        // TODO: Remove
        // self.client
        //     .query("DROP TABLE IF EXISTS ?")
        //     .bind(sql::Identifier(&self.name))
        //     .execute()
        //     .await?;
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

    // TODO: WIP this needs to be refactored... how?
    pub async fn index(&self) -> Result<()> {
        // make sure table exists
        self.create().await?;

        let (tx, mut rx) = mpsc::channel(10);
        let mut downloader = self.downloader.clone();

        tokio::spawn(async move {
            downloader.download_with_channel(Some(tx)).await?;
            Ok::<_, anyhow::Error>(())
        });

        let mut i = 0;
        while let Some(file) = rx.recv().await {
            let mut inserter = self
                .client
                .inserter::<CHRow>(&self.name)?
                .with_max_rows(100_000)
                .with_period(Some(Duration::from_secs(15)));

            log::debug!(
                "Indexing pair={}; file={}",
                file.pair,
                file.path.to_string_lossy()
            );

            // TODO: How do I refactor this well?
            let mut file_reader = BufReader::new(File::open(file.path).await?);
            let mut zip = ZipFileReader::with_tokio(&mut file_reader).await?;
            for entry in zip.file().entries() {
                entry.filename().as_str()?;
            }
            for index in 0..zip.file().entries().len() {
                // TODO: Verify the correct filename from zip
                let entry = zip.file().entries().get(index).unwrap();
                log::info!("Zip File: {}", entry.filename().as_str()?);
                let mut reader = zip
                    .reader_without_entry(index)
                    .await
                    .context("Failed to read ZipEntry")?
                    .compat();

                let mut rdr = csv_async::AsyncReaderBuilder::new()
                    .has_headers(false)
                    .create_deserializer(&mut reader);

                // TODO: How do I refactor this so that Row can be specified by File?
                let mut records = rdr.deserialize::<FileRow>();
                while let Some(row) = records.next().await {
                    let row = row?;
                    inserter.write(&CHRow::new(&file.pair, row))?;
                }
                // commit file
                let stats = inserter.commit().await?;
                if stats.rows > 0 {
                    log::info!(
                        "[Commit] {} bytes, {} rows, {} transactions have been inserted",
                        stats.bytes,
                        stats.rows,
                        stats.transactions,
                    );
                }
            }
            let stats = inserter.end().await?;
            if stats.rows > 0 {
                log::info!(
                    "[Flush] {} bytes, {} rows, {} transactions have been inserted",
                    stats.bytes,
                    stats.rows,
                    stats.transactions,
                );
            }
            i += 1;
        }

        log::info!("Total indexed: {}", i);

        Ok(())
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
