use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
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
    bucket: Bucket,
    checksum: Object,
    object: Object,
    pub path: PathBuf,
}

impl File {
    pub fn new(object: Object, checksum: Object) -> Result<Self> {
        let config = config::Config::create();
        let bucket = Bucket::new().map_err(|e| anyhow!("Failed to create bucket: {}", e))?;
        let data_dir = Path::new(config.data.dir.trim_end_matches('/'));
        let path = data_dir.join(object.key.replace("data/", "binance/"));
        let path = shellexpand::full(path.to_str().unwrap())
            .map_err(|e| anyhow!("Failed to expand path: {}", e))?;
        let path = Path::new(path.as_ref()).to_path_buf();

        Ok(File {
            bucket,
            object,
            checksum,
            path,
        })
    }

    async fn is_downloaded(&self) -> Result<bool> {
        let exists = fs::try_exists(&self.path).await.with_context(|| {
            format!(
                "Could not check file exists: {}",
                &self.path.to_string_lossy()
            )
        })?;
        Ok(exists)
    }

    pub async fn download(&self) -> Result<()> {
        if self.is_downloaded().await? {
            return Ok(());
        }

        // TODO: download into /tmp first and move to prevent unfinished downloads
        self.bucket
            .get_object_to_file(&self.object.key, &self.path)
            .await
            .with_context(|| {
                format!(
                    "Could not download object to file: {} -> {}",
                    self.object.key,
                    self.path.to_string_lossy()
                )
            })?;

        if !self.checksum_matches().await? {
            fs::remove_file(&self.path).await?;
            return Err(anyhow!(
                "Checksum does not match, removing file: {}",
                self.path.to_string_lossy()
            ));
        };

        log::debug!(
            "Downloaded: {} -> {}",
            self.object.key,
            self.path.to_string_lossy()
        );

        Ok(())
    }

    async fn checksum_matches(&self) -> Result<bool> {
        let bucket_sha_string = self.bucket.read_object(&self.checksum.key).await?;
        let bucket_sha = bucket_sha_string.split(' ').next().unwrap();
        let disk_sha = self.sha256_digest().await?;
        Ok(bucket_sha.eq_ignore_ascii_case(&disk_sha))
    }

    // TODO: Refactor to utilities
    async fn sha256_digest(&self) -> Result<String> {
        let input = fs::File::open(&self.path).await?;
        let mut reader = BufReader::new(input);

        let digest = {
            let mut hasher = Sha256::new();
            let mut buffer = [0; 8192];
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
