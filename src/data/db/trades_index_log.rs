use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use clickhouse::{sql, Client, Row};
use serde::{Deserialize, Serialize};

use super::utils::create_client;

#[derive(Clone)]
pub struct TradesIndexLogTable {
    client: Client,
    database: Arc<str>,
    name: Arc<str>,
}

impl TradesIndexLogTable {
    pub async fn new(database: &str) -> Result<Self> {
        Ok(TradesIndexLogTable {
            client: create_client(database).await?,
            database: Arc::from(database),
            name: "TRADES_INDEX_LOG".into(),
        })
    }
    pub async fn create(&self) -> Result<()> {
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?
                (
                    filename String COMMENT 'basename ==> name.ext', 
                    start_id UInt32 COMMENT 'Id FROM which this file has indexed data',
                    end_id UInt32 COMMENT 'Id UNTIL which this file has indexed data',
                    start_period_dt DateTime64(3, 'UTC') COMMENT 'Instant datetime (dt) (inclusive) FROM which this file has indexed data', 
                    end_period_dt DateTime64(3, 'UTC') COMMENT 'Instant datetime (dt) (inclusive) UNTIL which this file has indexed data', 
                    database String COMMENT 'Database name containing the table into which records have been indexed to',
                    table String COMMENT 'Table name into which the records have been indexed to',
                    num_rows UInt32 COMMENT 'Number of rows indexed from this file',
                    index_dt DateTime64(3, 'UTC') COMMENT 'Datetime (dt) when file was indexed in ms',
                )
                ENGINE = ReplacingMergeTree(index_dt)
                PRIMARY KEY (filename, start_id, table)
                ORDER BY (filename, start_id, table)
                ",
            )
            .bind(sql::Identifier(&self.name))
            .execute()
            .await
            .map_err(|e| anyhow!("Could not create table: {}", e))
    }

    pub async fn index_row(&self, row: FileIndexLogRow) -> Result<()> {
        // TODO: Db initialization procedure otw this will get called multiple times
        self.create().await?;

        let mut insert = self.client.insert(&self.name)?;
        insert
            .write(&row)
            .await
            .with_context(|| format!("Could not write row into {}.{}", self.database, self.name))?;
        insert.end().await.map_err(|e| {
            anyhow!(
                "Could not finish inserting into {}.{}: {}",
                self.database,
                self.name,
                e
            )
        })
    }
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct FileIndexLogRow {
    /// Filename: basename ==> name.ext
    pub filename: String,
    /// id (inclusive) from which this file has data indexed
    pub start_id: u32,
    /// id (inclusive) until which this file has data indexed
    pub end_id: u32,
    /// Instant in epoch time ms (inclusive) from which this file has data indexed
    pub start_period_dt: u64,
    /// Instant in epoch time ms (inclusive) until which this file has data indexed
    pub end_period_dt: u64,
    /// Database  name containing the table into which this file was indexed
    pub database: String,
    /// Table name into which this file was indexed
    pub table: String,
    /// Number of rows indexed from this file
    pub num_rows: u32,
    /// Datetime instant when this file finished indexing
    pub index_dt: u64,
}
