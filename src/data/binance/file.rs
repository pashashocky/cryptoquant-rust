use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use log::info;
use s3::serde_types::Object;
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    io::{AsyncReadExt, BufReader},
};

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

    async fn sha256_digest(&self) -> Result<String> {
        let input = fs::File::open(&self.path).await?;
        let mut reader = BufReader::new(input);

        let digest = {
            let mut hasher = Sha256::new();
            let mut buffer = [0; 1024];
            loop {
                let count = reader.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                hasher.update(&buffer[..count]);
            }
            hasher.finalize()
        };
        Ok(format!("{:X}", digest))
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
        info!(
            "Calculated [{}] {:?}",
            self.path.to_string_lossy(),
            self.sha256_digest().await.unwrap()
        );
        let bucket_sha = bucket.bucket.get_object(&self.checksum.key).await?;
        let bucket_sha = bucket_sha.as_str().unwrap().split(' ').next().unwrap();
        info!("Read [{}] {:?}", self.checksum.key, bucket_sha);
        Ok(())
    }
}
