use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;

use super::errors::Result;
use super::ObjStoreAccess;

#[derive(Debug, Clone)]
pub struct AzureStoreAccess {
    /// Container name for Azure store.
    pub container: String,
    /// Account name for Azure store.
    pub account_name: Option<String>,
    /// Access key for Azure store account.
    pub access_key: Option<String>,
    /// Other options for Azure store.
    pub opts: HashMap<AzureConfigKey, String>,
}

impl Display for AzureStoreAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Azure(container: {})", self.container)
    }
}

impl ObjStoreAccess for AzureStoreAccess {
    fn base_url(&self) -> Result<ObjectStoreUrl> {
        let u = format!("azure://{}", self.container);
        let u = ObjectStoreUrl::parse(u)?;
        Ok(u)
    }

    fn create_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let mut builder = MicrosoftAzureBuilder::new();

        for (key, val) in self.opts.iter() {
            builder = builder.with_config(*key, val);
        }

        if let Some(account_name) = &self.account_name {
            builder = builder.with_account(account_name);
        }

        if let Some(access_key) = &self.access_key {
            builder = builder.with_access_key(access_key);
        }

        let build = builder.with_container_name(&self.container).build()?;
        Ok(Arc::new(build))
    }

    fn path(&self, location: &str) -> Result<ObjectStorePath> {
        Ok(ObjectStorePath::from_url_path(location)?)
    }
}
