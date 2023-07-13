use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use errors::ObjectStoreSourceError;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

use errors::Result;

mod csv;
pub mod errors;
pub mod gcs;
pub mod http;
mod json;
pub mod local;
pub mod parquet;
pub mod registry;
pub mod s3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileType {
    Csv,
    Parquet,
    Json,
}

impl FromStr for FileType {
    type Err = ObjectStoreSourceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "parquet" => Ok(Self::Parquet),
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            _ => Err(Self::Err::NotSupportFileType(s)),
        }
    }
}

#[async_trait::async_trait]
pub trait TableAccessor: Send + Sync {
    fn store(&self) -> &Arc<dyn ObjectStore>;

    fn object_meta(&self) -> &Arc<ObjectMeta>;
    /// returns the base path.
    /// Example:
    /// - s3://bucket_name/path/to/file.parquet -> s3://bucket_name
    /// - http://domain.com/path/to/file.parquet -> http://domain.com
    fn base_path(&self) -> String;
    /// returns the location of the file.
    /// Example:
    /// - s3://bucket_name/path/to/file.parquet -> path/to/file.parquet
    fn location(&self) -> String;
    async fn into_table_provider(self, predicate_pushdown: bool) -> Result<Arc<dyn TableProvider>>;
}

pub fn file_type_from_path(path: &ObjectStorePath) -> Result<FileType> {
    path.extension()
        .ok_or(ObjectStoreSourceError::NoFileExtension)?
        .parse()
}
