use std::sync::Arc;

use datafusion::datasource::TableProvider;
use metastore::types::catalog::ConnectionOptionsS3;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::csv::CsvTableProvider;
use crate::errors::Result;
use crate::parquet::ParquetTableProvider;
use crate::{file_type_from_path, FileType, TableAccessor};

/// Information needed for accessing an external Parquet file on Amazon S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3TableAccess {
    /// S3 object store region
    pub region: String,
    /// S3 object store bucket name
    pub bucket_name: String,
    /// S3 object store access key id
    pub access_key_id: String,
    /// S3 object store secret access key
    pub secret_access_key: String,
    /// S3 object store table location
    pub location: String,
    pub file_type: Option<FileType>,
}

#[derive(Debug)]
pub struct S3Accessor {
    /// S3 object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
    pub file_type: FileType,
}

impl TableAccessor for S3Accessor {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn object_meta(&self) -> &Arc<ObjectMeta> {
        &self.meta
    }
}

impl S3Accessor {
    /// Setup accessor for S3
    pub async fn new(access: S3TableAccess) -> Result<Self> {
        let store = Arc::new(
            AmazonS3Builder::new()
                .with_region(access.region)
                .with_bucket_name(access.bucket_name)
                .with_access_key_id(access.access_key_id)
                .with_secret_access_key(access.secret_access_key)
                .build()?,
        );

        let location = ObjectStorePath::from(access.location);
        // Use provided file type or infer from location
        let file_type = access.file_type.unwrap_or(file_type_from_path(&location)?);
        trace!(?location, ?file_type, "location and file type");

        let meta = Arc::new(store.head(&location).await?);

        Ok(Self {
            store,
            meta,
            file_type,
        })
    }

    pub async fn validate_connection(options: &ConnectionOptionsS3) -> Result<()> {
        Ok(())
    }

    pub async fn validate_table_access(access: &S3TableAccess) -> Result<()> {
        let access = access.to_owned();
        let store = Arc::new(
            AmazonS3Builder::new()
                .with_region(access.region)
                .with_bucket_name(access.bucket_name)
                .with_access_key_id(access.access_key_id)
                .with_secret_access_key(access.secret_access_key)
                .build()?,
        );

        let location = ObjectStorePath::from(access.location);
        store.head(&location).await?;
        Ok(())
    }

    pub async fn into_table_provider(
        self,
        predicate_pushdown: bool,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_provider: Arc<dyn TableProvider> = match self.file_type {
            FileType::Parquet => {
                Arc::new(ParquetTableProvider::from_table_accessor(self, predicate_pushdown).await?)
            }
            FileType::Csv => Arc::new(CsvTableProvider::from_table_accessor(self).await?),
        };
        Ok(table_provider)
    }
}
