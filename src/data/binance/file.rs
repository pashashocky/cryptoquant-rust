use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
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

    pub async fn download(&self) -> Result<()> {
        if self.is_downloaded().await? {
            return Ok(());
        }

        let bucket = Bucket::new().map_err(|e| anyhow!("Failed to create bucket: {}", e))?;
        bucket
            .get_object_to_file(&self.object.key, &self.path)
            .await?;

        if !self.checksum_matches().await? {
            log::error!(
                "Checksum does not match {}, removing file!",
                self.path.to_string_lossy()
            );
            fs::remove_file(&self.path).await?;
        };
        Ok(())
    }

    async fn checksum_matches(&self) -> Result<bool> {
        let bucket = Bucket::new().map_err(|e| anyhow!("Failed to create bucket: {}", e))?;
        let bucket_sha_string = bucket.read_object(&self.checksum.key).await?;
        let bucket_sha = bucket_sha_string.split(' ').next().unwrap().to_lowercase();
        let disk_sha = self.sha256_digest_lower().await?;
        Ok(bucket_sha.eq(disk_sha.as_str()))
    }

    async fn sha256_digest_lower(&self) -> Result<String> {
        // TODO: Refactor to utilities
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
}
