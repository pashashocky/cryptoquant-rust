use anyhow::{anyhow, Result};

use super::{file_collection::FileCollection, s3::Bucket};

#[derive(Debug, Clone)]
pub struct Pair {
    pub prefix: String,
    pub name: String,
    bucket: Bucket,
}

impl Pair {
    pub fn new<T: Into<String>>(prefix: T, name: T) -> Result<Self> {
        let bucket = Bucket::new().map_err(|e| anyhow!("Failed to create bucket: {}", e))?;
        Ok(Pair {
            prefix: prefix.into(),
            name: name.into(),
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
