use std::path::PathBuf;

use serde::Deserialize;

#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreKind {
    #[default]
    LocalTemporary,
    Local {
        object_store_path: String,
    },
    Memory,
    Gcs {
        service_account_path: String,
        bucket_name: String,
    },
    S3,
}

#[derive(Default, Debug, Deserialize)]
pub struct AccessConfig {
    pub db_name: String,
    pub object_store: ObjectStoreKind,
    pub cached: bool,
    pub max_object_store_cache_size: Option<u64>,
    pub cache_path: Option<PathBuf>,
}
