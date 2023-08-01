use std::fmt::Display;
use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;

use super::errors::{ObjectStoreSourceError, Result};
use super::ObjStoreAccess;

#[derive(Debug, Clone)]
pub struct S3StoreAccess {
    /// S3 object store region.
    pub region: String,
    /// Bucket name for S3 store.
    pub bucket: String,
    /// Access key ID for AWS.
    pub access_key_id: Option<String>,
    /// Secret access key to the key ID for AWS.
    pub secret_access_key: Option<String>,
}

impl Display for S3StoreAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S3(bucket: {}, region: {})", self.bucket, self.region)
    }
}

impl ObjStoreAccess for S3StoreAccess {
    fn base_url(&self) -> Result<ObjectStoreUrl> {
        let u = format!("s3://{}", self.bucket);
        let u = ObjectStoreUrl::parse(u)?;
        Ok(u)
    }

    fn create_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let builder = AmazonS3Builder::new()
            .with_region(&self.region)
            .with_bucket_name(&self.bucket);

        let builder = match (&self.access_key_id, &self.secret_access_key) {
            (Some(id), Some(secret)) => builder
                .with_access_key_id(id)
                .with_secret_access_key(secret),
            (None, None) => {
                // TODO: Null credentials.
                builder
            }
            _ => {
                return Err(ObjectStoreSourceError::Static(
                    "Access key id and secret must both be provided",
                ))
            }
        };

        let build = builder.build()?;
        Ok(Arc::new(build))
    }

    fn path(&self, location: &str) -> Result<ObjectStorePath> {
        Ok(ObjectStorePath::from_url_path(location)?)
    }
}
