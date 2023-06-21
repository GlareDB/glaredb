use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use errors::ObjectStoreSourceError;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

use errors::Result;

pub mod errors;
pub mod gcs;
pub mod http;
pub mod local;
pub mod parquet;
pub mod s3;

mod csv;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileType {
    Csv,
    Parquet,
}

impl FromStr for FileType {
    type Err = ObjectStoreSourceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "parquet" => Ok(Self::Parquet),
            "csv" => Ok(Self::Csv),
            _ => Err(Self::Err::NotSupportFileType(s)),
        }
    }
}

pub trait TableAccessor: Send + Sync {
    fn store(&self) -> &Arc<dyn ObjectStore>;

    fn object_meta(&self) -> &Arc<ObjectMeta>;
}

pub fn file_type_from_path(path: &ObjectStorePath) -> Result<FileType> {
    path.extension()
        .ok_or(ObjectStoreSourceError::NoFileExtension)?
        .parse()
}
