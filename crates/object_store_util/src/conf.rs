use object_store::{
    gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, memory::InMemory,
    path::Path as ObjectPath, prefix::PrefixStore, Error as ObjectStoreError, ObjectStore,
};
use std::path::PathBuf;
use std::sync::Arc;

/// Configuration options for various types of storage we support.
#[derive(Debug, Clone)]
pub enum StorageConfig {
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
            StorageConfig::Memory => Arc::new(InMemory::new()),
        })
    }
}
