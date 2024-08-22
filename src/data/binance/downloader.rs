use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures::future::try_join_all;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;

use super::data_types::{Asset, Cadence, DataType};
use super::file::File;
use super::file_collection::FileCollection;
use super::pair::Pair;
use super::s3::Bucket;

#[derive(Clone)]
pub struct Downloader {
    pub name: Arc<str>,
    pub asset: Asset,
    pub cadence: Cadence,
    pub data_type: DataType,
    pair_filter_excluded: Option<Vec<String>>,
    pair_filter_starts_with: Option<Vec<String>>,
    pair_filter_ends_with: Option<Vec<String>>,
    pairs: Vec<Pair>,
    files: FileCollection,
}

impl Downloader {
    pub fn new(name: &str, asset: Asset, cadence: Cadence, data_type: DataType) -> Result<Self> {
        match asset {
            Asset::Futures | Asset::Option => todo!("Futures | Option not implemented."),
            Asset::Spot => (),
        }

        match data_type {
            DataType::AggTrades | DataType::KLines => todo!("AggTrades | Klines not implemented."),
            DataType::Trades => (),
        }

        Ok(Self {
            name: Arc::from(name),
            asset,
            cadence,
            data_type,
            pair_filter_excluded: None,
            pair_filter_starts_with: None,
            pair_filter_ends_with: None,
            pairs: Vec::new(),
            files: FileCollection::empty(),
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

        log::info!("[{}] Fetching pairs from: {}", self.name, &path);
        let bucket = Bucket::new().map_err(|e| anyhow!("Failed to create bucket: {}", e))?;
        let mut pairs = bucket.list_pairs(&path).await?;

        pairs.retain(|p| {
            let mut has_filters = false;

            if let Some(excluded_filters) = &self.pair_filter_excluded {
                if excluded_filters.iter().any(|f| p.name.contains(f)) {
                    return false;
                }
            }

            if let Some(starts_with_filters) = &self.pair_filter_starts_with {
                has_filters = true;
                if starts_with_filters.iter().any(|f| p.name.starts_with(f)) {
                    return true;
                }
            }

            if let Some(ends_with_filters) = &self.pair_filter_ends_with {
                has_filters = true;
                if ends_with_filters.iter().any(|f| p.name.ends_with(f)) {
                    return true;
                }
            }

            // If we have filters, we want the default to exclude ==> false
            // If no filters, we want the default to return all ==> true
            !has_filters
        });

        log::info!("[{}] Found {} pairs to download.", self.name, pairs.len());
        self.pairs = pairs;
        Ok(self)
    }

    // TODO: make configurable semaphore
    pub async fn get_files(&mut self) -> Result<&mut Self> {
        let semaphore = Arc::new(Semaphore::new(100));

        // new pairs can be introduced between long running jobs
        self.get_pairs().await?;

        let tasks: Vec<_> = self
            .pairs
            .iter()
            .map(|pair| {
                let semaphore = semaphore.clone();
                let pair = pair.clone();
                let downloader_name = self.name.clone();

                tokio::spawn(async move {
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
                })
            })
            .collect();

        let results = try_join_all(tasks).await?;
        let files = results.into_iter().flatten().collect::<FileCollection>();

        log::info!(
            "[{}] Found a total of {} objects from {} pairs",
            self.name,
            files.len(),
            self.pairs.len()
        );

        self.files = files;
        Ok(self)
    }

    pub async fn download_with_channel(&mut self, tx: Option<Sender<File>>) -> Result<()> {
        // new files can be introduced between long running jobs
        self.get_files().await?;

        self.files.download(tx).await?;
        Ok(())
    }

    pub async fn download(&mut self) -> Result<()> {
        self.download_with_channel(None).await
    }
}
