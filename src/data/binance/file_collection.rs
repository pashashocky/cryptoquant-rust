use std::collections::HashMap;

use std::iter::FromIterator;

use anyhow::{anyhow, Result};
use futures::stream::StreamExt;
use futures::Stream;
use s3::serde_types::Object;

use super::file::File;

#[derive(Debug, Default, Clone)]
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
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow!("Could not create FileCollection from objects: {}", e))?;

        Ok(FileCollection::new(files))
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }

    pub fn download_stream(&self, num_semaphore: usize) -> impl Stream<Item = Result<File>> {
        futures::stream::iter(self.files.clone())
            .map(|file| async move {
                match file.download().await {
                    Ok(_) => Ok(file),
                    Err(e) => {
                        log::error!("Could not download file. {}", e);
                        Err(anyhow::anyhow!("Failed to download file: {}", e))
                    }
                }
            })
            .buffer_unordered(num_semaphore)
    }
}

impl FromIterator<FileCollection> for FileCollection {
    fn from_iter<T: IntoIterator<Item = FileCollection>>(iter: T) -> Self {
        let files = iter.into_iter().fold(Vec::new(), |mut acc, collection| {
            acc.extend(collection.files);
            acc
        });
        FileCollection::new(files)
    }
}

impl FromIterator<File> for FileCollection {
    fn from_iter<T: IntoIterator<Item = File>>(iter: T) -> Self {
        let files: Vec<File> = iter.into_iter().collect();
        FileCollection::new(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils;

    #[test]
    fn file_collection_is_normal() {
        test_utils::is_normal::<FileCollection>();
    }
}
