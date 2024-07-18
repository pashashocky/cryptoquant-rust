use std::collections::HashMap;
use std::fmt::Display;

use anyhow::{Context, Result};
use casey::lower;
use log::info;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::serde_types::Object;

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
        let region = "ap-northeast-1".parse().context("Failed to parse region")?;
        let credentials =
            Credentials::anonymous().context("Failed to create anonymous credentials")?;
        let bucket = Self::create_bucket(region, credentials)?;

        let path = format!(
            "data/{}/{}/{}/{}/",
            asset.name(),
            cadence.name(),
            data_type.name(),
            &pair
        );

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

    fn create_bucket(region: s3::region::Region, credentials: Credentials) -> Result<Bucket> {
        let mut bucket = Bucket::new("data.binance.vision", region, credentials)
            .context("Failed to create S3 bucket")?
            .with_path_style();

        bucket.set_listobjects_v2();
        Ok(bucket)
    }

    /// Fetches files from the S3 bucket and groups them by their key prefix.
    pub async fn get_files(&mut self) -> Result<()> {
        info!("Fetching {:#?}", self.path);
        let objects = self.fetch_objects().await?;
        let grouped_objects = self.group_objects_by_prefix(objects);
        let files = self.collect_files(grouped_objects);

        info!("{:#?}", files);
        info!("Fetched {}", files.len());

        self.files = Some(files);
        Ok(())
    }

    async fn fetch_objects(&self) -> Result<Vec<Object>> {
        let objects = self
            .bucket
            .list(self.path.clone(), Some("/".to_string()))
            .await
            .context("Failed to list s3 bucket objects")?
            .into_iter()
            .flat_map(|result| result.contents)
            .collect::<Vec<Object>>();
        Ok(objects)
    }

    fn group_objects_by_prefix(
        &self,
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

    fn collect_files(
        &self,
        grouped_objects: HashMap<String, (Option<Object>, Option<Object>)>,
    ) -> Vec<File> {
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
}
