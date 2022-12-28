use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
pub struct ObjectStoreConfig {
    pub db_name: String,
    pub object_store: ObjectStoreKind,
    pub cached: bool,
    pub max_object_store_cache_size: Option<u64>,
    pub cache_path: Option<PathBuf>,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            db_name: String::from("glaredb"),
            object_store: Default::default(),
            cached: true,
            max_object_store_cache_size: Some(1073741824), // 1 GiB
            cache_path: Some(PathBuf::from("/tmp")),
        }
    }
}
