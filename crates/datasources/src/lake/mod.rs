//! Implementations for various data lakes.

pub mod delta;
pub mod iceberg;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use protogen::metastore::types::options::StorageOptions;
use std::str::FromStr;
use std::sync::Arc;

use crate::common::url::{DatasourceUrl, DatasourceUrlType};

#[derive(Debug, thiserror::Error)]
pub enum LakeStorageOptionsError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    Common(#[from] crate::common::errors::DatasourceCommonError),

    #[error("Missing host in data source url: {0:?}")]
    MissingHost(DatasourceUrl),

    #[error("Unsupported object store for url: {0:?}")]
    UnsupportedObjectStore(DatasourceUrl),
}

/// Create an object store from the provided storage options.
pub fn storage_options_into_object_store(
    url: &DatasourceUrl,
    opts: &StorageOptions,
) -> Result<Arc<dyn ObjectStore>, LakeStorageOptionsError> {
    match url.datasource_url_type() {
        DatasourceUrlType::S3 => {
            let bucket = url
                .host()
                .ok_or_else(|| LakeStorageOptionsError::MissingHost(url.clone()))?;

            let mut store = AmazonS3Builder::new().with_bucket_name(bucket);

            for (key, value) in &opts.inner {
                if let Ok(s3_key) = AmazonS3ConfigKey::from_str(key) {
                    store = store.with_config(s3_key, value);
                }
            }
            Ok(Arc::new(store.build()?))
        }
        DatasourceUrlType::Gcs => {
            let bucket = url
                .host()
                .ok_or_else(|| LakeStorageOptionsError::MissingHost(url.clone()))?;

            let mut store = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

            for (key, value) in &opts.inner {
                if let Ok(gcp_key) = GoogleConfigKey::from_str(key) {
                    store = store.with_config(gcp_key, value);
                }
            }
            Ok(Arc::new(store.build()?))
        }
        DatasourceUrlType::Azure => {
            let bucket = url
                .host()
                .ok_or_else(|| LakeStorageOptionsError::MissingHost(url.clone()))?;

            let mut store = MicrosoftAzureBuilder::new().with_container_name(bucket);

            for (key, value) in &opts.inner {
                if let Ok(azure_key) = AzureConfigKey::from_str(key) {
                    store = store.with_config(azure_key, value);
                }
            }

            Ok(Arc::new(store.build()?))
        }
        DatasourceUrlType::File => {
            let store = LocalFileSystem::new();
            Ok(Arc::new(store))
        }
        DatasourceUrlType::Http => {
            Err(LakeStorageOptionsError::UnsupportedObjectStore(url.clone()))
        }
    }
}
