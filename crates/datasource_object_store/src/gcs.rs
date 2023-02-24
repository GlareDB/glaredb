use std::sync::Arc;

use datafusion::datasource::TableProvider;
use metastore::types::catalog::ConnectionOptionsGcs;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::csv::CsvTableProvider;
use crate::errors::Result;
use crate::parquet::ParquetTableProvider;
use crate::{file_type_from_path, FileType, TableAccessor};

/// Information needed for accessing an external Parquet file on Google Cloud
/// Storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsTableAccess {
    /// GCS object store bucket name
    pub bucket_name: String,
    /// GCS object store service account key
    pub service_acccount_key_json: String,
    /// GCS object store table location
    pub location: String,
    pub file_type: Option<FileType>,
}

#[derive(Debug)]
pub struct GcsAccessor {
    /// GCS object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
    pub file_type: FileType,
}

impl TableAccessor for GcsAccessor {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn object_meta(&self) -> &Arc<ObjectMeta> {
        &self.meta
    }
}

impl GcsAccessor {
    /// Setup accessor for GCS
    pub async fn new(access: GcsTableAccess) -> Result<Self> {
        let store = Self::create_store(access.service_acccount_key_json, access.bucket_name)?;

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

    fn create_store(
        service_acccount_key_json: String,
        bucket_name: String,
    ) -> Result<Arc<dyn ObjectStore>> {
        Ok(Arc::new(
            GoogleCloudStorageBuilder::new()
                .with_service_account_key(service_acccount_key_json)
                .with_bucket_name(bucket_name)
                .build()?,
        ))
    }

    pub async fn validate_connection(options: &ConnectionOptionsGcs) -> Result<()> {
        Ok(())
    }

    pub async fn validate_table_access(access: &GcsTableAccess) -> Result<()> {
        let store = Self::create_store(
            access.service_acccount_key_json.to_owned(),
            access.bucket_name.to_owned(),
        )?;

        let location = ObjectStorePath::from(access.location.to_owned());
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
