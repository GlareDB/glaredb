//! Implementations for various data lakes.

pub mod delta;
pub mod iceberg;

use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use protogen::metastore::types::options::{CredentialsOptionsAws, CredentialsOptionsGcp};
use std::collections::HashMap;
use std::sync::Arc;

use crate::common::url::DatasourceUrl;

#[derive(Debug, thiserror::Error)]
pub enum LakeStorageOptionsError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    Common(#[from] crate::common::errors::DatasourceCommonError),

    #[error("Missing host in data source url: {0:?}")]
    MissingHost(DatasourceUrl),
}

/// Options required for each of GCS/S3/local when accessing Iceberg and Delta
/// tables.
#[derive(Debug, Clone)]
pub enum LakeStorageOptions {
    S3 {
        creds: CredentialsOptionsAws,
        region: String,
    },
    Gcs {
        creds: CredentialsOptionsGcp,
    },
    Local, // Nothing needed for local.
}

impl LakeStorageOptions {
    /// Turn self into a hashmap containing object_store specific options. This
    /// hashmap is passed into delta-rs which will then create the appropriate
    /// object store for us using these options.
    ///
    /// - [Azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants)
    /// - [S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants)
    /// - [Google options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants)
    pub fn into_opts_hashmap(self) -> HashMap<String, String> {
        let mut opts = HashMap::new();
        match self {
            Self::S3 { creds, region } => {
                opts.insert("aws_access_key_id".to_string(), creds.access_key_id);
                opts.insert("aws_secret_access_key".to_string(), creds.secret_access_key);
                opts.insert("aws_region".to_string(), region);
            }
            Self::Gcs { creds } => {
                opts.insert(
                    "google_service_account_key".to_string(),
                    creds.service_account_key,
                );
            }
            Self::Local => (),
        }
        opts
    }

    /// Create an object store from self.
    pub fn into_object_store(
        self,
        url: &DatasourceUrl,
    ) -> Result<Arc<dyn ObjectStore>, LakeStorageOptionsError> {
        match self {
            Self::S3 { creds, region } => {
                let bucket = url
                    .host()
                    .ok_or_else(|| LakeStorageOptionsError::MissingHost(url.clone()))?;

                let store = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_access_key_id(creds.access_key_id)
                    .with_secret_access_key(creds.secret_access_key)
                    .build()?;
                Ok(Arc::new(store))
            }
            LakeStorageOptions::Gcs { creds } => {
                let bucket = url
                    .host()
                    .ok_or_else(|| LakeStorageOptionsError::MissingHost(url.clone()))?;

                let store = GoogleCloudStorageBuilder::new()
                    .with_bucket_name(bucket)
                    .with_service_account_key(creds.service_account_key)
                    .build()?;
                Ok(Arc::new(store))
            }
            LakeStorageOptions::Local => {
                let store = LocalFileSystem::new();
                Ok(Arc::new(store))
            }
        }
    }
}
