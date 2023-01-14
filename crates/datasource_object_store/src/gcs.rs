use std::sync::Arc;

use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::errors::Result;
use crate::parquet::ParquetTableProvider;
use crate::TableAccessor;

/// Information needed for accessing an external Parquet file on Google Cloud Storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsTableAccess {
    /// GCS object store bucket name
    pub bucket_name: String,
    /// GCS object store service account key
    pub service_acccount_key_json: String,
    /// GCS object store table location
    pub location: String,
}

#[derive(Debug)]
pub struct GcsAccessor {
    /// GCS object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
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
        let store = Arc::new(
            GoogleCloudStorageBuilder::new()
                .with_service_account_key(access.service_acccount_key_json)
                .with_bucket_name(access.bucket_name)
                .build()?,
        );

        let location = ObjectStorePath::from(access.location);
        let meta = Arc::new(store.head(&location).await?);

        Ok(Self { store, meta })
    }

    pub async fn into_table_provider(
        self,
        predicate_pushdown: bool,
    ) -> Result<ParquetTableProvider<GcsAccessor>> {
        ParquetTableProvider::from_table_accessor(self, predicate_pushdown).await
    }
}
