use std::{ops::Deref, path::Path, sync::Arc};

use anyhow::{anyhow, Context, Result};
use async_zip::tokio::read::seek::ZipFileReader;
use csv_async::DeserializeRecordsIntoStream;
use serde::{
    de::{self, Unexpected},
    Deserialize, Deserializer, Serialize,
};
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    io::{AsyncRead, AsyncReadExt, BufReader},
};
use tokio_util::compat::FuturesAsyncReadCompatExt;

use super::s3::Bucket;
use crate::utils::config;

trait DeserializableFromCSV<'r> {
    fn into_deserialize_from_csv_reader<R: AsyncRead + Send + Unpin + 'r>(
        reader: R,
    ) -> csv_async::DeserializeRecordsIntoStream<'r, R, Self>
    where
        Self: Sized;
}

// https://github.com/BurntSushi/rust-csv/issues/135#issuecomment-1058584727
fn bool_from_str<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer)?.as_str() {
        "true" | "True" => Ok(true),
        "false" | "False" => Ok(false),
        other => Err(de::Error::invalid_value(
            Unexpected::Str(other),
            &"Must be truthy (true, True) or falsey (false, False)",
        )),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Row {
    /// Trade id
    pub id: u32,
    /// Execution price in DENOM
    pub price: f32,
    /// Trade quantity in BASE
    pub qty: f32,
    /// Notional value; price * qty
    pub quote_qty: f32,
    /// Trade time in unix epoch to ms
    pub time: u64,
    /// Is the buyer the maker in this trade ==> true is a short trade
    #[serde(deserialize_with = "bool_from_str")]
    pub is_buyer_maker: bool,
    /// Was this the best price available on the exchange?
    #[serde(skip)]
    pub is_best_match: bool,
}

impl<'r> DeserializableFromCSV<'r> for Row {
    fn into_deserialize_from_csv_reader<R: AsyncRead + Send + Unpin + 'r>(
        reader: R,
    ) -> csv_async::DeserializeRecordsIntoStream<'r, R, Self>
    where
        Self: Sized,
    {
        csv_async::AsyncReaderBuilder::new()
            .has_headers(false)
            .create_deserializer(reader)
            .into_deserialize()
    }
}

#[derive(Debug, Clone)]
pub struct File {
    checksum_key: Arc<str>,
    object_key: Arc<str>,
    pub pair: Arc<str>,
    pub path: Arc<Path>,
}

impl File {
    pub fn new(pair: &str, object_key: &str, checksum_key: &str) -> Result<Self> {
        let config = config::Config::create();
        let data_dir = Path::new(config.data.dir.trim_end_matches('/'));

        let path = data_dir.join(object_key.replace("data/", "binance/"));
        let path = shellexpand::full(path.to_str().unwrap())
            .map_err(|e| anyhow!("Failed to expand path: {}", e))?;
        let path = Path::new(path.as_ref()).to_path_buf();

        Ok(File {
            object_key: Arc::from(object_key),
            checksum_key: Arc::from(checksum_key),
            pair: Arc::from(pair),
            path: Arc::from(path),
        })
    }

    async fn is_downloaded(&self) -> Result<bool> {
        let exists = fs::try_exists(&self.path).await.with_context(|| {
            format!(
                "Could not check file exists: {}",
                &self.path.to_string_lossy()
            )
        })?;
        // TODO: We need a mechanism to verify that this file is not being downloaded
        // by some other process / thread at this moment in time
        // - check checksum
        // - name files being downloaded as .download like in Chrome
        Ok(exists)
    }

    pub async fn download(&self) -> Result<()> {
        if self.is_downloaded().await? {
            return Ok(());
        }

        // TODO: download into /tmp first and move to prevent unfinished downloads
        let bucket = Bucket::new()?;
        bucket
            .get_object_to_file(&self.object_key, self.path.deref())
            .await?;

        if !self.checksum_matches().await? {
            fs::remove_file(&self.path).await?;
            return Err(anyhow!(
                "Checksum does not match, removing file: {}",
                self.path.to_string_lossy()
            ));
        };

        log::debug!(
            "Downloaded: {} -> {}",
            self.object_key,
            self.path.to_string_lossy()
        );

        Ok(())
    }

    pub async fn records<'r>(
        &self,
    ) -> Result<DeserializeRecordsIntoStream<'r, Box<dyn AsyncRead + Send + Unpin>, Row>> {
        let file = fs::File::open(&self.path).await?;
        let file_reader = BufReader::new(file);
        let zip = ZipFileReader::with_tokio(file_reader).await?;
        let index = match zip.file().entries().len() {
            1 => 0,
            num => {
                return Err(anyhow!(
                    "The zip file has {} files, expected 1. {}",
                    num,
                    self.path.to_string_lossy()
                ))
            }
        };
        let reader =
            Box::new(zip.into_entry(index).await?.compat()) as Box<dyn AsyncRead + Unpin + Send>;
        Ok(Row::into_deserialize_from_csv_reader(reader))
    }

    async fn checksum_matches(&self) -> Result<bool> {
        let bucket = Bucket::new()?;
        let bucket_sha_string = bucket.read_object(&self.checksum_key).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils;

    #[test]
    fn file_is_normal() {
        test_utils::is_normal::<File>();
    }
}
