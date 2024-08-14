use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use s3::serde_types::Object;
use tokio::sync::Semaphore;

use super::file::File;

pub struct FileCollection {
    files: Vec<File>,
}

impl FileCollection {
    pub fn new(files: Vec<File>) -> Self {
        FileCollection { files }
    }

    // Assumes objects are stored in pairs
    // - name.zip
    // - name.zip.CHECKSUM
    pub fn from_objects(objects: Vec<Object>, checksum_suffix: &str) -> Result<Self> {
        // Create a HashMap to group objects by prefix
        let grouped_objects: HashMap<String, (Option<Object>, Option<Object>)> =
            objects.into_iter().fold(HashMap::new(), |mut map, object| {
                let key = object.key.clone();
                let prefix = if key.ends_with(checksum_suffix) {
                    &key[..key.len() - checksum_suffix.len()]
                } else {
                    &key
                };

                let entry = map.entry(prefix.to_string()).or_default();
                if key.ends_with(checksum_suffix) {
                    entry.1 = Some(object);
                } else {
                    entry.0 = Some(object);
                }

                map
            });

        // Create a FileCollection from the grouped objects
        let files = grouped_objects
            .into_iter()
            .map(|(_, (object, checksum))| match (object, checksum) {
                (Some(object), Some(checksum)) => File::new(object, checksum),
                _ => Err(anyhow!("Missing an object or a checksum")),
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(FileCollection::new(files))
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }

    pub fn extend(&mut self, other: FileCollection) -> &mut Self {
        self.files.extend(other.files);
        self
    }

    pub async fn download(&self) -> Result<()> {
        // TODO: make configurable semaphore
        let semaphore = Arc::new(Semaphore::new(50));
        let mut handles = Vec::new();

        for file in self.files.iter() {
            let semaphore = semaphore.clone();
            let file = file.clone();

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await?;
                file.download().await
            });

            handles.push(handle);
        }

        for handle in handles {
            match handle.await? {
                Ok(_) => (),
                Err(e) => log::error!("Could not download file. {}", e),
            }
        }
        Ok(())
    }
}
