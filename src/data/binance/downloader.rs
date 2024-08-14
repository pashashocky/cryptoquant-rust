use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::Semaphore;

use super::data_types::{Asset, Cadence, DataType};
use super::file_collection::FileCollection;
use super::s3::Bucket;

#[derive(Debug, Clone)]
pub struct Pair {
    pub prefix: String,
    pub name: String,
    bucket: Bucket,
}

impl Pair {
    pub fn new<T: Into<String> + Display>(prefix: T, name: T) -> Result<Self> {
        let bucket = Bucket::new().map_err(|e| anyhow!("Failed to create bucket: {}", e))?;
        Ok(Pair {
            prefix: prefix.into(),
            name: name.into(),
            bucket,
        })
    }

    async fn get_files(&self) -> Result<FileCollection> {
        let objects = self
            .bucket
            .list_objects(&self.prefix)
            .await
            .map_err(|e| anyhow!("Could not list bucket objects: {}", e))?;
        let files = FileCollection::from_objects(objects, ".CHECKSUM")
            .map_err(|e| anyhow!("Could not create FileCollection from objects: {}", e))?;

        Ok(files)
    }
}

pub struct Downloader {
    pub name: String,
    pub asset: Asset,
    pub cadence: Cadence,
    pub data_type: DataType,
    bucket: Bucket,
    pair_filter_excluded: Option<Vec<String>>,
    pair_filter_starts_with: Option<Vec<String>>,
    pair_filter_ends_with: Option<Vec<String>>,
    pairs: Option<Vec<Pair>>,
    files: Option<FileCollection>,
}

impl Downloader {
    pub fn new(name: String, asset: Asset, cadence: Cadence, data_type: DataType) -> Result<Self> {
        match asset {
            Asset::Futures | Asset::Option => todo!("Futures | Option not implemented."),
            Asset::Spot => (),
        }

        let bucket = Bucket::new().map_err(|e| anyhow!("Failed to create bucket: {}", e))?;

        Ok(Self {
            name,
            asset,
            cadence,
            data_type,
            bucket,
            pair_filter_excluded: None,
            pair_filter_starts_with: None,
            pair_filter_ends_with: None,
            pairs: None,
            files: None,
        })
    }

    pub fn with_pair_excluded(mut self, pairs: &[&str]) -> Self {
        let pairs: Vec<String> = pairs.iter().map(|p| p.to_string()).collect();
        self.pair_filter_excluded = Some(pairs);
        self
    }

    pub fn with_pair_starts_with(mut self, pairs: &[&str]) -> Self {
        let pairs: Vec<String> = pairs.iter().map(|p| p.to_string()).collect();
        self.pair_filter_starts_with = Some(pairs);
        self
    }

    pub fn with_pair_ends_with(mut self, pairs: &[&str]) -> Self {
        let pairs: Vec<String> = pairs.iter().map(|p| p.to_string()).collect();
        self.pair_filter_ends_with = Some(pairs);
        self
    }

    pub async fn get_pairs(&mut self) -> Result<&mut Self> {
        let path = Path::new("data")
            .join(&self.asset)
            .join(&self.cadence)
            .join(&self.data_type)
            .to_string_lossy()
            .to_string();

        let mut pairs = self.bucket.list_pairs(&path).await?;

        pairs.retain(|p| {
            if let Some(excluded_filters) = &self.pair_filter_excluded {
                if excluded_filters.iter().any(|f| p.name.contains(f)) {
                    return false;
                }
            }

            if let Some(starts_with_filters) = &self.pair_filter_starts_with {
                if starts_with_filters.iter().any(|f| p.name.starts_with(f)) {
                    return true;
                }
            }

            if let Some(ends_with_filters) = &self.pair_filter_ends_with {
                if ends_with_filters.iter().any(|f| p.name.ends_with(f)) {
                    return true;
                }
            }

            false
        });

        log::info!("[{}] Found {} pairs to download.", self.name, pairs.len());
        self.pairs = Some(pairs);
        Ok(self)
    }

    pub async fn get_files(&mut self) -> Result<&mut Self> {
        // TODO: make configurable semaphore
        let semaphore = Arc::new(Semaphore::new(100));
        let mut handles = Vec::new();

        if self.pairs.is_none() {
            self.get_pairs().await?;
        }

        for pair in self.pairs.iter().flatten() {
            let semaphore = semaphore.clone();
            let pair = pair.clone();
            let downloader_name = self.name.clone();

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await?;
                log::info!(
                    "[{}] Getting objects for {} from: {}",
                    downloader_name,
                    pair.name,
                    pair.prefix
                );

                let files = pair.get_files().await?;
                log::info!(
                    "[{}] Discovered {} objects for {} from: {}",
                    downloader_name,
                    files.len(),
                    pair.name,
                    pair.prefix
                );
                Ok::<_, anyhow::Error>(files)
            });

            handles.push(handle);
        }

        let mut files = FileCollection::new(vec![]);
        for handle in handles {
            let pair_files = handle.await??;
            files.extend(pair_files);
        }

        log::info!(
            "[{}] Found a total of {} objects from {} pairs",
            self.name,
            files.len(),
            self.pairs.as_ref().unwrap().len()
        );
        self.files = Some(files);
        Ok(self)
    }

    pub async fn download(&mut self) -> Result<()> {
        if self.files.is_none() {
            self.get_files().await?;
        }

        match &self.files {
            Some(files) => files.download().await?,
            None => log::info!("No files, call `get_files` first."),
        }
        Ok(())
    }
}
