use std::sync::Arc;

use datafusion::datasource::TableProvider;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tracing::trace;

use super::csv::CsvTableProvider;
use super::errors::Result;
use super::json::JsonTableProvider;
use super::parquet::ParquetTableProvider;
use super::{file_type_from_path, FileType, TableAccessor};

/// Information needed for accessing an external Parquet file on Google Cloud
/// Storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsTableAccess {
    /// GCS object store bucket name
    pub bucket_name: String,
    /// GCS object store service account key
    pub service_acccount_key_json: Option<String>,
    /// GCS object store table location
    pub location: String,
    /// Optionally specify file type for the table.
    pub file_type: Option<FileType>,
}

impl GcsTableAccess {
    fn builder(&self) -> GoogleCloudStorageBuilder {
        let builder = GoogleCloudStorageBuilder::new().with_bucket_name(&self.bucket_name);
        match &self.service_acccount_key_json {
            Some(key) => builder.with_service_account_key(key),
            None => {
                // TODO: Null Credentials
                builder
            }
        }
    }

    pub fn store_and_path(&self) -> Result<(Arc<dyn ObjectStore>, ObjectStorePath)> {
        let store = self.builder().build()?;

        let location = ObjectStorePath::from_url_path(&self.bucket_name).unwrap();
        Ok((Arc::new(store), location))
    }

    pub fn into_object_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let store = self.builder().build()?;
        Ok(Arc::new(store))
    }

    fn location(&self) -> ObjectStorePath {
        ObjectStorePath::from_url_path(&self.location).unwrap()
    }
}

#[derive(Debug)]
pub struct GcsAccessor {
    /// GCS object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
    pub file_type: FileType,
    base_url: String,
}

#[async_trait::async_trait]
impl TableAccessor for GcsAccessor {
    fn base_path(&self) -> String {
        format!("gs://{}", self.base_url)
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

impl GcsAccessor {
    /// Setup accessor for GCS
    pub async fn new(access: GcsTableAccess) -> Result<Self> {
        let store = access.into_object_store()?;
        // Use provided file type or infer from location
        let location = access.location();

        let file_type = access.file_type.unwrap_or(file_type_from_path(&location)?);
        trace!(?location, ?file_type, "location and file type");

        let meta = Arc::new(store.head(&location).await?);
        Ok(Self {
            store,
            meta,
            file_type,
            base_url: access.bucket_name,
        })
    }

    pub async fn validate_table_access(access: GcsTableAccess) -> Result<()> {
        let store = Arc::new(access.builder().build()?);
        let location = access.location();

        store.head(&location).await?;
        Ok(())
    }
}
