//! Implementations for various data lakes.

pub mod delta;
pub mod iceberg;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::aws::AmazonS3ConfigKey;
use object_store::azure::AzureConfigKey;
use object_store::gcp::GoogleConfigKey;
use object_store::ObjectStore;
use protogen::metastore::types::options::StorageOptions;

use crate::common::url::{DatasourceUrl, DatasourceUrlType};
use crate::object_store::azure::AzureStoreAccess;
use crate::object_store::gcs::GcsStoreAccess;
use crate::object_store::local::LocalStoreAccess;
use crate::object_store::s3::S3StoreAccess;
use crate::object_store::ObjStoreAccess;

#[derive(Debug, thiserror::Error)]
pub enum LakeStorageOptionsError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStoreSource(#[from] crate::object_store::errors::ObjectStoreSourceError),

    #[error(transparent)]
    Common(#[from] crate::common::errors::DatasourceCommonError),

    #[error("Missing host in data source url: {0:?}")]
    MissingHost(DatasourceUrl),

    #[error("Unsupported object store for url: {0:?}")]
    UnsupportedObjectStore(DatasourceUrl),
}

pub fn storage_options_into_store_access(
    url: &DatasourceUrl,
    opts: &StorageOptions,
) -> Result<Arc<dyn ObjStoreAccess>, LakeStorageOptionsError> {
    match url.datasource_url_type() {
        DatasourceUrlType::S3 => {
            let bucket = url
                .host()
                .ok_or_else(|| LakeStorageOptionsError::MissingHost(url.clone()))?;

            let mut s3_opts = HashMap::new();
            for (key, value) in &opts.inner {
                if let Ok(s3_key) = AmazonS3ConfigKey::from_str(key) {
                    s3_opts.insert(s3_key, value.to_string());
                }
            }

            Ok(Arc::new(S3StoreAccess {
                bucket: bucket.to_string(),
                opts: s3_opts,
                region: None,
                access_key_id: None,
                secret_access_key: None,
            }))
        }
        DatasourceUrlType::Gcs => {
            let bucket = url
                .host()
                .ok_or_else(|| LakeStorageOptionsError::MissingHost(url.clone()))?;

            let mut gcs_opts = HashMap::new();
            for (key, value) in &opts.inner {
                if let Ok(gcs_key) = GoogleConfigKey::from_str(key) {
                    gcs_opts.insert(gcs_key, value.to_string());
                }
            }

            Ok(Arc::new(GcsStoreAccess {
                bucket: bucket.to_string(),
                opts: gcs_opts,
                service_account_key: None,
            }))
        }
        DatasourceUrlType::Azure => {
            let container = url
                .host()
                .ok_or_else(|| LakeStorageOptionsError::MissingHost(url.clone()))?;

            let mut azure_opts = HashMap::new();
            for (key, value) in &opts.inner {
                if let Ok(azure_key) = AzureConfigKey::from_str(key) {
                    azure_opts.insert(azure_key, value.to_string());
                }
            }

            Ok(Arc::new(AzureStoreAccess {
                container: container.to_string(),
                opts: azure_opts,
                account_name: None,
                access_key: None,
            }))
        }
        DatasourceUrlType::File => Ok(Arc::new(LocalStoreAccess)),
        DatasourceUrlType::Http => {
            Err(LakeStorageOptionsError::UnsupportedObjectStore(url.clone()))
        }
    }
}

/// Create an object store from the provided storage options.
pub fn storage_options_into_object_store(
    url: &DatasourceUrl,
    opts: &StorageOptions,
) -> Result<Arc<dyn ObjectStore>, LakeStorageOptionsError> {
    let access = storage_options_into_store_access(url, opts)?;
    Ok(access.create_store()?)
}
