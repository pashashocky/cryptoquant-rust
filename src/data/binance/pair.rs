use std::sync::Arc;

use anyhow::{anyhow, Result};

use super::{file_collection::FileCollection, s3::Bucket};

#[derive(Debug, Clone)]
pub struct Pair {
    pub prefix: Arc<str>,
    pub name: Arc<str>,
    bucket: Bucket,
}

impl Pair {
    pub fn new(prefix: &str, name: &str) -> Result<Self> {
        let bucket = Bucket::new().map_err(|e| anyhow!("Failed to create bucket: {}", e))?;
        Ok(Pair {
            prefix: Arc::from(prefix),
            name: Arc::from(name),
            bucket,
        })
    }

    pub async fn get_files(&self) -> Result<FileCollection> {
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
