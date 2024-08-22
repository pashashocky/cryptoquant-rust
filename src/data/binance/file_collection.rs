use std::{collections::HashMap, sync::Arc};

use std::iter::FromIterator;

use anyhow::{anyhow, Result};
use futures::stream::StreamExt;
use s3::serde_types::Object;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;

use super::file::File;

#[derive(Default, Clone)]
pub struct FileCollection {
    files: Vec<File>,
}

impl FileCollection {
    pub fn empty() -> Self {
        FileCollection::default()
    }

    pub fn new(files: Vec<File>) -> Self {
        FileCollection { files }
    }

    // Assumes objects are stored in pairs
    // - name.zip
    // - name.zip.CHECKSUM
    pub fn from_objects(pair: &str, objects: Vec<Object>, checksum_suffix: &str) -> Result<Self> {
        // Create a HashMap to group objects by prefix
        let grouped_objects: HashMap<String, (Option<Object>, Option<Object>)> =
            objects.into_iter().fold(HashMap::new(), |mut map, object| {
                let key = &object.key;
                let prefix = if key.ends_with(checksum_suffix) {
                    &key[..key.len() - checksum_suffix.len()]
                } else {
                    key
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
                (Some(object), Some(checksum)) => File::new(pair, &object.key, &checksum.key),
                _ => Err(anyhow!("Missing an object or a checksum")),
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(FileCollection::new(files))
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }

    // TODO: make configurable semaphore
    pub async fn download(&self, tx: Option<Sender<File>>) -> Result<()> {
        let num_semaphore = 50;
        let semaphore = Arc::new(Semaphore::new(num_semaphore));

        futures::stream::iter(&self.files)
            .for_each_concurrent(num_semaphore, |file| {
                let semaphore = semaphore.clone();
                let tx = tx.clone();
                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    match file.download().await {
                        Ok(_) => {
                            if let Some(tx) = tx {
                                tx.send(file.clone()).await.unwrap()
                            }
                        }
                        Err(e) => log::error!("Could not download file. {}", e),
                    }
                }
            })
            .await;

        Ok(())
    }
}

impl FromIterator<FileCollection> for FileCollection {
    fn from_iter<T: IntoIterator<Item = FileCollection>>(iter: T) -> Self {
        let mut files = Vec::new();
        for collection in iter {
            files.extend(collection.files);
        }
        FileCollection::new(files)
    }
}

impl FromIterator<File> for FileCollection {
    fn from_iter<T: IntoIterator<Item = File>>(iter: T) -> Self {
        let files: Vec<File> = iter.into_iter().collect();
        FileCollection::new(files)
    }
}
