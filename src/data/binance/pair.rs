use std::sync::Arc;

use anyhow::Result;

use super::{file_collection::FileCollection, s3::Bucket};

#[derive(Debug, Clone)]
pub struct Pair {
    pub prefix: Arc<str>,
    pub name: Arc<str>,
}

impl Pair {
    pub fn new(prefix: &str, name: &str) -> Result<Self> {
        Ok(Pair {
            prefix: Arc::from(prefix),
            name: Arc::from(name),
        })
    }

    pub async fn get_files(&self) -> Result<FileCollection> {
        let bucket = Bucket::new()?;
        let objects = bucket.list_objects(&self.prefix).await?;
        let files = FileCollection::from_objects(&self.name, objects, ".CHECKSUM")?;

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils;

    #[test]
    fn pair_is_normal() {
        test_utils::is_normal::<Pair>();
    }
}
