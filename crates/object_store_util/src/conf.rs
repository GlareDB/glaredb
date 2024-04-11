use std::path::PathBuf;
use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, S3CopyIfNotExists};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{Error as ObjectStoreError, ObjectStore};

/// Configuration options for various types of storage we support.
#[derive(Debug, Clone, PartialEq)]
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
        bucket: Option<String>,
    },
    Azure {
        account_name: String,
        access_key: String,
        container_name: Option<String>,
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
                    if endpoint.contains("r2.cloudflarestorage.com") {
                        // Ensure `ObjectStore::copy_if_not_exists` is enabled on the S3 client for
                        // Cloudflare R2 with the adequate header
                        builder = builder.with_copy_if_not_exists(S3CopyIfNotExists::Header(
                            "cf-copy-destination-if-none-match".to_string(),
                            "*".to_string(),
                        ))
                    }
                }

                if let Some(bucket) = bucket {
                    builder = builder.with_bucket_name(bucket);
                }

                Arc::new(builder.build()?)
            }
            StorageConfig::Gcs {
                service_account_key,
                bucket,
            } => {
                let mut builder =
                    GoogleCloudStorageBuilder::new().with_service_account_key(service_account_key);

                if let Some(bucket) = bucket {
                    builder = builder.with_bucket_name(bucket);
                }

                Arc::new(builder.build()?)
            }
            StorageConfig::Azure {
                account_name,
                access_key,
                container_name,
            } => {
                let mut builder = MicrosoftAzureBuilder::new()
                    .with_account(account_name)
                    .with_access_key(access_key);

                if let Some(container_name) = container_name {
                    builder = builder.with_container_name(container_name);
                }

                Arc::new(builder.build()?)
            }
            StorageConfig::Local { path } => Arc::new(LocalFileSystem::new_with_prefix(path)?),
            StorageConfig::Memory => Arc::new(InMemory::new()),
        })
    }
}
