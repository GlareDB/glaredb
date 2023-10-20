use std::fmt::Display;
use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;

use super::errors::{ObjectStoreSourceError, Result};
use super::ObjStoreAccess;

#[derive(Debug, Clone)]
pub struct AzureStoreAccess {
    /// Bucket name for Azure. Azure uses the term "container" for this.
    pub bucket: String,
    /// Account where the container lives.
    pub account: Option<String>,
    /// Access key for the account.
    pub access_key: Option<String>,
}

impl Display for AzureStoreAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Azure(bucket: {})", self.bucket)
    }
}

impl ObjStoreAccess for AzureStoreAccess {
    fn base_url(&self) -> Result<ObjectStoreUrl> {
        let u = format!("azure://{}", self.bucket);
        let u = ObjectStoreUrl::parse(u)?;
        Ok(u)
    }

    fn create_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let (account, access_key) = match (&self.account, &self.access_key) {
            (Some(account), Some(key)) => (account, key),
            _ => {
                return Err(ObjectStoreSourceError::Static(
                    "Account and access key must both be provided",
                ))
            }
        };

        let store = MicrosoftAzureBuilder::new()
            .with_access_key(access_key)
            .with_account(account)
            .with_container_name(&self.bucket)
            .build()?;
        Ok(Arc::new(store))
    }

    fn path(&self, location: &str) -> Result<ObjectStorePath> {
        Ok(ObjectStorePath::from_url_path(location)?)
    }
}
