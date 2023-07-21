//! Implementations for various data lakes.

pub mod delta;
pub mod iceberg;

use metastore_client::types::options::{CredentialsOptionsAws, CredentialsOptionsGcp};
use std::collections::HashMap;

/// Options required for each of GCS/S3/local.
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
    fn into_opts_hashmap(self) -> HashMap<String, String> {
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
}
