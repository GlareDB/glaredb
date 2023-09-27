use object_store::aws::AmazonS3Builder;
use object_store::{
    gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, memory::InMemory,
    Error as ObjectStoreError, ObjectStore,
};
use once_cell::sync::Lazy;
use std::path::PathBuf;
use std::sync::Arc;

static IN_MEMORY_STORE: Lazy<Arc<InMemory>> = Lazy::new(|| Arc::new(InMemory::new()));

/// Configuration options for various types of storage we support.
#[derive(Debug, Clone)]
pub enum StorageConfig {
    S3 {
        access_key_id: String,
        secret_access_key: String,
        region: Option<String>,
        endpoint: Option<String>,
        bucket: Option<String>,
    },
    Gcs {
        service_account_key: String,
        bucket: String,
    },
    Local {
        path: PathBuf,
    },
    Memory,
}

impl StorageConfig {
    /// Create a new object store using this config.
    pub fn new_object_store(&self) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        Ok(match self {
            StorageConfig::S3 {
                access_key_id,
                secret_access_key,
                region,
                endpoint,
                bucket,
            } => {
                let mut builder = AmazonS3Builder::new()
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key)
                    .with_region(region.clone().unwrap_or_default());

                if let Some(endpoint) = endpoint {
                    if endpoint.starts_with("http://") {
                        builder = builder.with_allow_http(true);
                    }
                    builder = builder.with_endpoint(endpoint);
                }

                if let Some(bucket) = bucket {
                    builder = builder.with_bucket_name(bucket);
                }

                Arc::new(builder.build()?)
            }
            StorageConfig::Gcs {
                service_account_key,
                bucket,
            } => Arc::new(
                GoogleCloudStorageBuilder::new()
                    .with_bucket_name(bucket)
                    .with_service_account_key(service_account_key)
                    .build()?,
            ),
            StorageConfig::Local { path } => Arc::new(LocalFileSystem::new_with_prefix(path)?),
            StorageConfig::Memory => IN_MEMORY_STORE.clone(),
        })
    }
}
