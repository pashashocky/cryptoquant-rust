use std::path::Path;

use anyhow::{anyhow, Context, Result};
use s3::{creds::Credentials, serde_types::Object, Bucket as S3Bucket};
use tokio::fs;

use crate::utils::config;

use super::pair::Pair;

#[derive(Debug)]
pub struct Bucket {
    bucket: S3Bucket,
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

    pub async fn get_object_to_file(&self, key: &str, file_path: &Path) -> Result<()> {
        // create parent dirs
        match file_path.parent() {
            Some(path) if !path.exists() => fs::create_dir_all(path).await.with_context(|| {
                format!("Failed to create directory: {}", path.to_string_lossy())
            })?,
            None => return Err(anyhow!("{} has no parent", file_path.to_string_lossy())),
            _ => (),
        };

        let mut output_file = fs::File::create_new(file_path).await?;
        self.bucket
            .get_object_to_writer(key, &mut output_file)
            .await
            .with_context(|| {
                format!(
                    "Could not download object to file: {} -> {}",
                    key,
                    file_path.to_string_lossy()
                )
            })?;
        Ok(())
    }

    pub async fn list_pairs(&self, path: &str) -> Result<Vec<Pair>> {
        let terminated_path = if path.ends_with('/') {
            path.to_owned()
        } else {
            format!("{}/", path)
        };

        self.bucket
            .list(terminated_path, Some("/".to_string()))
            .await
            .with_context(|| {
                anyhow!(
                    "Failed to list S3 bucket objects from: {}/",
                    path.trim_end_matches('/'),
                )
            })?
            .into_iter()
            .flat_map(|result| result.common_prefixes.unwrap_or_default())
            .map(|cp| {
                let pair_name = cp.prefix.rsplit_terminator('/').next().unwrap_or_default();
                Pair::new(&cp.prefix, pair_name)
            })
            .collect::<Result<Vec<_>>>()
    }

    pub async fn list_objects(&self, path: &str) -> Result<Vec<Object>> {
        let terminated_path = if path.ends_with('/') {
            path.to_owned()
        } else {
            format!("{}/", path)
        };

        let objects = self
            .bucket
            .list(terminated_path, Some("/".to_string()))
            .await
            .with_context(|| {
                format!(
                    "Failed to list s3 bucket objects from: {}/",
                    path.trim_end_matches('/'),
                )
            })?
            .into_iter()
            .flat_map(|result| result.contents)
            .collect::<Vec<Object>>();
        Ok(objects)
    }

    pub async fn read_object(&self, path: &str) -> Result<String> {
        self.bucket
            .get_object(&path)
            .await
            .with_context(|| format!("Could not read object: {}", path))?
            .to_string()
            .with_context(|| format!("Could not convert object contents to String: {}", path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils;

    #[test]
    fn bucket_is_normal() {
        test_utils::is_normal::<Bucket>();
    }
}
