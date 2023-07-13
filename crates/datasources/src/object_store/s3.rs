use std::sync::Arc;

use datafusion::datasource::TableProvider;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tracing::trace;

use super::csv::CsvTableProvider;
use super::errors::{ObjectStoreSourceError, Result};
use super::json::JsonTableProvider;
use super::parquet::ParquetTableProvider;
use super::{file_type_from_path, FileType, TableAccessor};

/// Information needed for accessing an external Parquet file on Amazon S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3TableAccess {
    /// S3 object store region
    pub region: String,
    /// S3 object store bucket name
    pub bucket_name: String,
    /// S3 object store access key id
    pub access_key_id: Option<String>,
    /// S3 object store secret access key
    pub secret_access_key: Option<String>,
    /// S3 object store table location
    pub location: String,
    pub file_type: Option<FileType>,
}

impl S3TableAccess {
    fn builder(&self) -> Result<AmazonS3Builder> {
        let builder = AmazonS3Builder::new()
            .with_region(&self.region)
            .with_bucket_name(&self.bucket_name);
        match (&self.access_key_id, &self.secret_access_key) {
            (Some(id), Some(secret)) => Ok(builder
                .with_access_key_id(id)
                .with_secret_access_key(secret)),
            (None, None) => Ok(builder),
            _ => Err(ObjectStoreSourceError::Static(
                "Access key id and secret must both be provided",
            )),
        }
    }

    pub fn store_and_path(&self) -> Result<(Arc<dyn ObjectStore>, ObjectStorePath)> {
        let store = self.builder()?.build()?;
        let location = ObjectStorePath::from_url_path(&self.location)?;
        Ok((Arc::new(store), location))
    }
}

#[derive(Debug)]
pub struct S3Accessor {
    /// S3 object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
    pub file_type: FileType,
    bucket: String,
}

#[async_trait::async_trait]
impl TableAccessor for S3Accessor {
    fn base_path(&self) -> String {
        format!("s3://{}/", self.bucket)
    }
    fn location(&self) -> String {
        self.meta.location.to_string()
    }
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn object_meta(&self) -> &Arc<ObjectMeta> {
        &self.meta
    }

    async fn into_table_provider(self, predicate_pushdown: bool) -> Result<Arc<dyn TableProvider>> {
        let table_provider: Arc<dyn TableProvider> = match self.file_type {
            FileType::Parquet => {
                Arc::new(ParquetTableProvider::from_table_accessor(self, predicate_pushdown).await?)
            }
            FileType::Csv => Arc::new(CsvTableProvider::from_table_accessor(self).await?),
            FileType::Json => Arc::new(JsonTableProvider::from_table_accessor(self).await?),
        };
        Ok(table_provider)
    }
}

impl S3Accessor {
    /// Setup accessor for S3
    pub async fn new(access: S3TableAccess) -> Result<Self> {
        let (store, location) = access.store_and_path()?;
        // Use provided file type or infer from location
        let file_type = access.file_type.unwrap_or(file_type_from_path(&location)?);
        trace!(?location, ?file_type, "location and file type");

        let meta = Arc::new(store.head(&location).await?);

        Ok(Self {
            store,
            meta,
            file_type,
            bucket: access.bucket_name,
        })
    }

    pub async fn validate_table_access(access: S3TableAccess) -> Result<()> {
        let (store, location) = access.store_and_path()?;
        store.head(&location).await?;
        Ok(())
    }
}
