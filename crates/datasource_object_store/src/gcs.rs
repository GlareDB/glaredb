use crate::csv::CsvTableProvider;
use crate::errors::Result;
use crate::listing::{ListingTableCreator, ObjectStoreHasher, HASH_QUERY_PARAM};
use crate::parquet::ParquetTableProvider;
use crate::{file_type_from_path, FileType, TableAccessor};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionConfig;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::trace;

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

impl ObjectStoreHasher for GcsTableAccess {
    fn generate_url(&self) -> url::Url {
        let mut hasher = DefaultHasher::new();
        self.service_acccount_key_json.hash(&mut hasher);
        let hash = hasher.finish();

        url::Url::parse(&format!(
            "gs://{0}/?{1}={2}",
            self.bucket_name, HASH_QUERY_PARAM, hash
        ))
        .unwrap()
    }

    fn build_object_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let store = Arc::new(
            GoogleCloudStorageBuilder::new()
                .with_service_account_key(&self.service_acccount_key_json)
                .with_bucket_name(&self.bucket_name)
                .build()?,
        );
        Ok(store)
    }
}

impl GcsTableAccess {
    fn location(&self) -> String {
        format!("gs://{0}/{1}", self.bucket_name, self.location)
    }
}

#[derive(Debug)]
pub struct GcsAccessor {
    pub access: GcsTableAccess,
}

impl GcsAccessor {
    /// Setup accessor for GCS
    pub async fn new(access: GcsTableAccess) -> Result<Self> {
        Ok(Self { access })
    }

    pub async fn validate_table_access(access: GcsTableAccess) -> Result<()> {
        let store = Arc::new(
            GoogleCloudStorageBuilder::new()
                .with_service_account_key(access.service_acccount_key_json)
                .with_bucket_name(access.bucket_name)
                .build()?,
        );

        let location = ObjectStorePath::from(access.location);
        store.head(&location).await?;
        Ok(())
    }

    pub async fn into_table_provider(self, state: &SessionState) -> Result<Arc<dyn TableProvider>> {
        let file_type = self.access.file_type.clone();
        let location = self.access.location();
        let table_provider = ListingTableCreator::new(state, self.access, file_type, &location)?;
        Ok(table_provider)
    }
}
