use anyhow::{Context, Result};
use clickhouse::inserter::Quantities;
use clickhouse::{sql, Client};
use std::ops::AddAssign;

use crate::utils::config;

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

pub async fn create_client(database: &str) -> Result<Client> {
    let cfg = config::Config::create().clickhouse;
    let database = &database.to_uppercase();

    Ok(Client::default()
        .with_url(cfg.url)
        .with_user(cfg.user)
        .with_password(cfg.password)
        .with_database(create_database(database).await?))
}

async fn create_database(database: &str) -> Result<&str> {
    let cfg = config::Config::create().clickhouse;
    let client = Client::default()
        .with_url(cfg.url)
        .with_user(cfg.user)
        .with_password(cfg.password);
    client
        .query("CREATE DATABASE IF NOT EXISTS ?")
        .bind(sql::Identifier(database))
        .execute()
        .await
        .with_context(|| format!("Could not create database: {}", database))?;
    Ok(database)
}
