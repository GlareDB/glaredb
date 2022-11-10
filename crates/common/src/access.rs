use std::{path::PathBuf, str::FromStr};

use serde::Deserialize;

use crate::errors::internal;

//TODO: Update these so they are from a config file rather than env variables
const GCS_SERVICE_ACCOUNT_PATH: &str = "GCS_SERVICE_ACCOUNT_PATH";
const BUCKET_NAME: &str = "GCS_BUCKET_NAME";

#[derive(Default, Debug, Deserialize)]
pub enum ObjectStoreKind {
    #[default]
    #[serde(alias = "local")]
    LocalTemp,
    Memory,
    Gcs {
        service_account_path: String,
        bucket_name: String,
    },
    S3,
}

impl FromStr for ObjectStoreKind {
    type Err = crate::errors::ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ObjectStoreKind::*;
        match s {
            "local" => Ok(LocalTemp),
            "memory" => Ok(Memory),
            "gcs" => {
                let service_account_path =
                    std::env::var(GCS_SERVICE_ACCOUNT_PATH).map_err(|e| {
                        internal!(
                            "GCS: {} environment variable error: {:?}",
                            GCS_SERVICE_ACCOUNT_PATH,
                            e
                        )
                    })?;
                let bucket_name = std::env::var(BUCKET_NAME).map_err(|e| {
                    internal!("GCS: {} environment variable error: {:?}", BUCKET_NAME, e)
                })?;
                Ok(Gcs {
                    service_account_path,
                    bucket_name,
                })
            }
            _ => Err(internal!(
                "This type of object storage is not supported: {}",
                s
            )),
        }
    }
}

//TODO: use new default, better yet ensure everything is set in config file
#[derive(Debug, Default, Deserialize)]
pub struct AccessConfig {
    pub db_name: String,
    pub object_store: ObjectStoreKind,
    pub cached: bool,
    pub max_object_store_cache_size: Option<u64>,
    pub cache_path: Option<PathBuf>,
}
