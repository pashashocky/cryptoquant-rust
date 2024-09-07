// TODO: replace with config crate from crates.io
use serde::{Deserialize, Serialize};
use std::{env, fs};

#[derive(Debug, Deserialize, Serialize)]
pub struct DataConfig {
    pub dir: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BinanceConfig {
    pub bucket_name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ClickhouseConfig {
    pub url: String,
    pub user: String,
    #[serde(default = "default_ch_password")]
    pub password: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub data: DataConfig,
    pub binance: BinanceConfig,
    pub clickhouse: ClickhouseConfig,
}

impl Config {
    pub fn create() -> Self {
        // Read the YAML file
        let config_content = fs::read_to_string("config.yaml").expect("Failed to read config.yaml");

        // Parse the YAML content into the Config struct
        let config: Config = serde_yaml::from_str(&config_content).expect("Failed to parse YAML");

        config
    }
}

fn default_ch_password() -> String {
    env::var("CLICKHOUSE_PASSWORD").unwrap_or_default()
}
