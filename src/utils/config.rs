use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Deserialize, Serialize)]
pub struct DataConfig {
    pub dir: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BinanceConfig {
    pub bucket_name: String,
}

impl Default for BinanceConfig {
    fn default() -> Self {
        BinanceConfig {
            bucket_name: "data.binance.vision".into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub data: DataConfig,
    pub binance: BinanceConfig,
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
