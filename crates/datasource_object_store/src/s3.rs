use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::errors::Result;
use crate::parquet::ParquetTableProvider;
use crate::TableAccessor;

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
}

#[derive(Debug)]
pub struct S3Accessor {
    /// S3 object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
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
        let meta = Arc::new(store.head(&location).await?);

        Ok(Self { store, meta })
    }

    pub async fn into_table_provider(
        self,
        predicate_pushdown: bool,
    ) -> Result<ParquetTableProvider<S3Accessor>> {
        ParquetTableProvider::from_table_accessor(self, predicate_pushdown).await
    }
}
