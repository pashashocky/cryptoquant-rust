use std::collections::HashMap;
use std::fmt::Display;

use anyhow::{anyhow, Context, Result};
use casey::lower;
use log::info;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::serde_types::Object;

use crate::utils::config;

macro_rules! pub_enum_str {
    (pub enum $name:ident {
        $($variant:ident),*,
    }) => {
        pub enum $name {
            $($variant),*
        }

        impl $name {
            fn name(&self) -> &'static str {
                match self {
                    $(Self::$variant => lower!(stringify!($variant))),*
                }
            }
        }
    };
}

pub_enum_str! {
    pub enum Asset {
        Futures,
        Option,
        Spot,
    }
}

pub_enum_str! {
    pub enum Cadence {
        Daily,
        Monthly,
    }
}

pub_enum_str! {
    pub enum DataType {
        AggTrades,
        KLines,
        Trades,
    }
}

#[derive(Debug)]
struct File {
    object: Object,
    checksum: Object,
}

// TODO: Refactor to S3 helpers
// START
async fn list_objects(bucket: &Bucket, path: &str) -> Result<Vec<Object>> {
    let objects = bucket
        .list(path.to_owned(), Some("/".to_string()))
        .await
        .context("Failed to list s3 bucket objects")?
        .into_iter()
        .flat_map(|result| result.contents)
        .collect::<Vec<Object>>();
    Ok(objects)
}

fn group_objects_by_prefix(
    objects: Vec<Object>,
) -> HashMap<String, (Option<Object>, Option<Object>)> {
    let mut grouped_objects: HashMap<String, (Option<Object>, Option<Object>)> = HashMap::new();

    for object in objects {
        let key = object.key.clone();
        let prefix = if key.ends_with(".CHECKSUM") {
            &key[..key.len() - ".CHECKSUM".len()]
        } else {
            &key
        };

        let entry = grouped_objects.entry(prefix.to_string()).or_default();
        if key.ends_with(".CHECKSUM") {
            entry.1 = Some(object);
        } else {
            entry.0 = Some(object);
        }
    }

    grouped_objects
}
// END

// TODO: Refactor to FileCollection struct
// START
fn collect_files(grouped_objects: HashMap<String, (Option<Object>, Option<Object>)>) -> Vec<File> {
    grouped_objects
        .into_iter()
        .filter_map(|(_, (object, checksum))| {
            if let (Some(object), Some(checksum)) = (object, checksum) {
                Some(File { object, checksum })
            } else {
                None
            }
        })
        .collect()
}
// END

pub struct BinanceHistory {
    pub bucket: Bucket,
    pub asset: Asset,
    pub cadence: Cadence,
    pub data_type: DataType,
    pub pair: String,
    path: String,
    files: Option<Vec<File>>,
}

impl BinanceHistory {
    /// Creates a new instance of `BinanceHistory`.
    ///
    /// # Arguments
    ///
    /// * `asset` - The type of asset (Futures, Option, Spot).
    /// * `cadence` - The cadence of the data (Daily, Monthly).
    /// * `data_type` - The type of data (AggTrades, KLines, Trades).
    /// * `pair` - The trading pair (e.g., "BTCUSDT").
    ///
    /// # Returns
    ///
    /// * `Result<BinanceHistory>` - The created `BinanceHistory` instance or an error.
    pub fn new<T: Into<String> + Display>(
        asset: Asset,
        cadence: Cadence,
        data_type: DataType,
        pair: T,
    ) -> Result<Self> {
        let bucket = Self::create_bucket()?;

        if pair.to_string().is_empty() {
            return Err(anyhow!("`pair` cannot be empty!"));
        }

        // TODO: Refactor to Path or something similar
        let path = format!(
            "data/{}/{}/{}/{}/",
            asset.name(),
            cadence.name(),
            data_type.name(),
            &pair
        );
        // END

        Ok(Self {
            bucket,
            asset,
            cadence,
            data_type,
            pair: pair.into(),
            path,
            files: None,
        })
    }

    /// Fetches files from the S3 bucket and groups them by their key prefix.
    pub async fn get_files(&mut self) -> Result<&mut Self> {
        info!("Fetching {:#?}", self.path);
        let objects = list_objects(&self.bucket, &self.path).await?;
        let grouped_objects = group_objects_by_prefix(objects);
        let files = collect_files(grouped_objects);

        info!("{:#?}", files);
        info!("Fetched {}", files.len());

        self.files = Some(files);
        Ok(self)
    }

    fn create_bucket() -> Result<Bucket> {
        let config = config::Config::create();
        let region = "ap-northeast-1".parse().context("Failed to parse region")?;
        let credentials =
            Credentials::anonymous().context("Failed to create anonymous credentials")?;
        let mut bucket = Bucket::new(config.binance.bucket_name.as_str(), region, credentials)
            .context("Failed to create S3 bucket")?
            .with_path_style();

        bucket.set_listobjects_v2();
        Ok(bucket)
    }
}
