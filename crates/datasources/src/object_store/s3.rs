use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential};
use object_store::path::Path as ObjectStorePath;
use object_store::{CredentialProvider, ObjectStore};

use super::errors::Result;
use super::ObjStoreAccess;

#[derive(Debug, Clone)]
pub struct S3StoreAccess {
    /// Bucket name for S3 store.
    pub bucket: String,
    /// S3 object store region.
    pub region: Option<String>,
    /// Access key ID for AWS.
    pub access_key_id: Option<String>,
    /// Secret access key to the key ID for AWS.
    pub secret_access_key: Option<String>,
    /// Other options for s3 store.
    pub opts: HashMap<AmazonS3ConfigKey, String>,
}

impl Display for S3StoreAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S3(bucket: {})", self.bucket)
    }
}

impl ObjStoreAccess for S3StoreAccess {
    fn base_url(&self) -> Result<ObjectStoreUrl> {
        let u = format!("s3://{}", self.bucket);
        let u = ObjectStoreUrl::parse(u)?;
        Ok(u)
    }

    fn create_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let mut builder = AmazonS3Builder::new();

        let mut creds = false;

        for (key, val) in self.opts.iter() {
            if matches!(
                key,
                AmazonS3ConfigKey::AccessKeyId | AmazonS3ConfigKey::SecretAccessKey
            ) {
                creds = true;
            }

            builder = builder.with_config(*key, val);
        }

        if let Some(access_key_id) = &self.access_key_id {
            creds = true;
            builder = builder.with_access_key_id(access_key_id);
        }

        if let Some(secret_access_key) = &self.secret_access_key {
            creds = true;
            builder = builder.with_secret_access_key(secret_access_key);
        }

        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }

        if !creds {
            builder = builder.with_credentials(Arc::new(NullCredentialProvider));
        }

        let build = builder.with_bucket_name(&self.bucket).build()?;

        Ok(Arc::new(build))
    }

    fn path(&self, location: &str) -> Result<ObjectStorePath> {
        Ok(ObjectStorePath::from_url_path(location)?)
    }
}

#[derive(Debug)]
struct NullCredentialProvider;

#[async_trait]
impl CredentialProvider for NullCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> Result<Arc<Self::Credential>, object_store::Error> {
        Ok(Arc::new(AwsCredential {
            key_id: String::new(),
            secret_key: String::new(),
            token: None,
        }))
    }
}
