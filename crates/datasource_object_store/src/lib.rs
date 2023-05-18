use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::datasource::file_format::file_type;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::errors::{ObjectStoreSourceError, Result};

pub mod errors;
pub mod gcs;
pub mod listing;
pub mod local;
pub mod s3;

mod csv;
mod parquet;

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
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
            _ => Err(ObjectStoreSourceError::UnsupportedFileType(s)),
        }
    }
}

impl From<FileType> for file_type::FileType {
    fn from(value: FileType) -> Self {
        match value {
            FileType::Csv => file_type::FileType::CSV,
            FileType::Parquet => file_type::FileType::PARQUET,
        }
    }
}

#[derive(Debug, Clone, Hash)]
pub enum FileCompressionType {
    Gzip,
    Uncompressed,
}

impl FromStr for FileCompressionType {
    type Err = ObjectStoreSourceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "gz" => Ok(Self::Gzip),
            _ => Err(ObjectStoreSourceError::UnsupportFileCompressionType(s)),
        }
    }
}

impl<S: AsRef<str>> TryFrom<Option<S>> for FileCompressionType {
    type Error = ObjectStoreSourceError;
    fn try_from(value: Option<S>) -> Result<Self> {
        match value {
            Some(s) => Self::from_str(s.as_ref()),
            None => Ok(FileCompressionType::Uncompressed),
        }
    }
}

impl From<FileCompressionType> for file_type::FileCompressionType {
    fn from(value: FileCompressionType) -> Self {
        match value {
            FileCompressionType::Gzip => file_type::FileCompressionType::GZIP,
            FileCompressionType::Uncompressed => file_type::FileCompressionType::UNCOMPRESSED,
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
