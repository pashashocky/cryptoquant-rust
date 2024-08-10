use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use log::info;
use s3::{creds::Credentials, serde_types::Object, Bucket as S3Bucket};
use tokio::fs;

use crate::utils::config;

pub struct Bucket {
    pub bucket: S3Bucket,
}

impl Bucket {
    pub fn new() -> Result<Self> {
        let config = config::Config::create();
        let region = "ap-northeast-1".parse().context("Failed to parse region")?;
        let credentials =
            Credentials::anonymous().context("Failed to create anonymous credentials")?;
        let mut bucket = S3Bucket::new(config.binance.bucket_name.as_str(), region, credentials)
            .context("Failed to create S3 bucket")?
            .with_path_style();
        bucket.set_listobjects_v2();

        Ok(Bucket { bucket })
    }

    pub async fn get_object_to_file(&self, key: &str, file_path: &PathBuf) -> Result<()> {
        // create parent dirs
        match file_path.parent() {
            Some(path) if !path.exists() => fs::create_dir_all(path)
                .await
                .context("Failed to create directory")?,
            None => return Err(anyhow!("{} has no parent", file_path.to_str().unwrap())),
            _ => (),
        };

        let mut output_file = fs::File::create_new(file_path).await?;
        self.bucket
            .get_object_to_writer(key, &mut output_file)
            .await
            .context("Failed to write object to file")?;

        info!("Downloaded: {} -> {}", key, file_path.to_string_lossy());

        Ok(())
    }

    pub async fn list_objects(&self, path: &str) -> Result<Vec<Object>> {
        let mut path = path.to_owned();
        if !path.ends_with('/') {
            path.push('/');
        }

        let objects = self
            .bucket
            .list(path, Some("/".to_string()))
            .await
            .context("Failed to list s3 bucket objects")?
            .into_iter()
            .flat_map(|result| result.contents)
            .collect::<Vec<Object>>();
        Ok(objects)
    }
}
