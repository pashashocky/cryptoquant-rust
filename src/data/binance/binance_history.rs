use std::fmt::Display;
use std::path::Path;

use anyhow::{anyhow, Result};
use log::info;

use super::data_types::{Asset, Cadence, DataType};
use super::file_collection::FileCollection;
use super::s3::Bucket;

pub struct BinanceHistory {
    pub bucket: Bucket,
    pub asset: Asset,
    pub cadence: Cadence,
    pub data_type: DataType,
    pub pair: String,
    path: String,
    files: Option<FileCollection>,
}

impl BinanceHistory {
    pub fn new<T: Into<String> + Display>(
        asset: Asset,
        cadence: Cadence,
        data_type: DataType,
        pair: T,
    ) -> Result<Self> {
        match asset {
            Asset::Futures | Asset::Option => todo!("Futures | Option not implemented."),
            Asset::Spot => (),
        }
        let bucket = Bucket::new().map_err(|e| anyhow!("Failed to create bucket: {}", e))?;

        if pair.to_string().is_empty() {
            return Err(anyhow!("`pair` cannot be empty!"));
        }

        let path = Path::new("data")
            .join(&asset)
            .join(&cadence)
            .join(&data_type)
            .join(pair.to_string());

        Ok(Self {
            bucket,
            asset,
            cadence,
            data_type,
            pair: pair.into(),
            path: path.to_string_lossy().into(),
            files: None,
        })
    }

    pub async fn get_files(&mut self) -> Result<&mut Self> {
        info!("Fetching {:#?}", self.path);
        let objects = self.bucket.list_objects(&self.path).await?;
        let files = FileCollection::from_objects(objects, ".CHECKSUM");

        info!("Fetched {} files.", files.len());

        self.files = Some(files);
        Ok(self)
    }

    pub async fn download(&self) -> Result<()> {
        match &self.files {
            Some(files) => files.download().await?,
            None => info!("No files, call `get_files` first."),
        }
        Ok(())
    }
}
