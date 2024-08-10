use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use s3::serde_types::Object;
use tokio::fs;

use super::s3::Bucket;
use crate::utils::config;

#[derive(Clone)]
pub struct File {
    checksum: Object,
    object: Object,
    pub path: PathBuf,
}

impl File {
    pub fn new(object: Object, checksum: Object) -> Result<Self> {
        let config = config::Config::create();
        let data_dir = Path::new(config.data.dir.trim_end_matches('/'));
        let path = data_dir.join(object.key.replace("data/", "binance/"));
        let path = shellexpand::full(path.to_str().unwrap())
            .map_err(|e| anyhow!("Failed to expand path: {}", e))?;
        let path = Path::new(path.as_ref()).to_path_buf();

        Ok(File {
            object,
            checksum,
            path,
        })
    }

    async fn is_downloaded(&self) -> Result<bool> {
        let exists = fs::try_exists(&self.path).await?;
        Ok(exists)
    }

    pub async fn download(&self) -> Result<()> {
        if self.is_downloaded().await? {
            return Ok(());
        }

        let bucket = Bucket::new().map_err(|e| anyhow!("Failed to create bucket: {}", e))?;
        bucket
            .get_object_to_file(&self.object.key, &self.path)
            .await?;

        // TODO: add CHECKSUM check
        Ok(())
    }
}
