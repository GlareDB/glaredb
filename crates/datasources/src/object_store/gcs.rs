use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::path::Path as ObjectStorePath;
use object_store::{CredentialProvider, ObjectStore};

use super::errors::Result;
use super::ObjStoreAccess;

#[derive(Debug, Clone)]
pub struct GcsStoreAccess {
    /// Bucket name for GCS store.
    pub bucket: String,
    /// Service account key (JSON) for credentials.
    pub service_account_key: Option<String>,
    /// Other options for GCS.
    pub opts: HashMap<GoogleConfigKey, String>,
}

impl Display for GcsStoreAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GCS(bucket: {})", self.bucket)
    }
}

impl ObjStoreAccess for GcsStoreAccess {
    fn base_url(&self) -> Result<ObjectStoreUrl> {
        let u = format!("gs://{}", self.bucket);
        let u = ObjectStoreUrl::parse(u)?;
        Ok(u)
    }

    fn create_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let mut builder = GoogleCloudStorageBuilder::new();

        let mut creds = false;

        for (key, val) in self.opts.iter() {
            if matches!(
                key,
                GoogleConfigKey::ServiceAccount | GoogleConfigKey::ServiceAccountKey
            ) {
                creds = true;
            }

            builder = builder.with_config(*key, val);
        }

        if let Some(service_account_key) = &self.service_account_key {
            creds = true;
            builder = builder.with_service_account_key(service_account_key);
        }

        if !creds {
            builder = builder.with_credentials(Arc::new(NullCredentialsProvider));
        }

        let build = builder.with_bucket_name(&self.bucket).build()?;
        Ok(Arc::new(build))
    }

    fn path(&self, location: &str) -> Result<ObjectStorePath> {
        Ok(ObjectStorePath::from_url_path(location)?)
    }
}

#[derive(Debug)]
struct NullCredentialsProvider;

#[async_trait]
impl CredentialProvider for NullCredentialsProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> Result<Arc<Self::Credential>, object_store::Error> {
        Ok(Arc::new(GcpCredential {
            bearer: String::new(),
        }))
    }
}
