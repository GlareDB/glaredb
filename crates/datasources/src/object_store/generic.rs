use std::fmt::Display;
use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;
use protogen::metastore::types::options::StorageOptions;

use super::errors::Result;
use super::ObjStoreAccess;
use crate::common::url::DatasourceUrl;
use crate::lake::storage_options_into_object_store;
use crate::object_store::errors::ObjectStoreSourceError;

/// A generic access implementation that supports a number of different object stores, as determined
/// by the provided storage options.
#[derive(Debug, Clone)]
pub struct GenericStoreAccess {
    /// Base URL of the object store, which must contain the scheme and the host (that can be empty)
    pub base_url: ObjectStoreUrl,
    /// Object store config options.
    pub storage_options: StorageOptions,
}

impl GenericStoreAccess {
    pub fn new_from_location_and_opts(
        location: &str,
        storage_options: StorageOptions,
    ) -> Result<Self> {
        // We want to generate a base URL from a potentially full URL here
        // TODO: If we can proto serialize URL consider doing this validating operation when processing
        // table definition
        let url = DatasourceUrl::try_new(location)
            .map_err(|_| ObjectStoreSourceError::Static("Couldn't parse data source location"))?;

        Ok(GenericStoreAccess {
            base_url: ObjectStoreUrl::try_from(url)?,
            storage_options: storage_options.clone(),
        })
    }
}

impl Display for GenericStoreAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Generic(base url: {})", self.base_url)
    }
}

impl ObjStoreAccess for GenericStoreAccess {
    fn base_url(&self) -> Result<ObjectStoreUrl> {
        Ok(self.base_url.clone())
    }

    fn create_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let datasource_url = DatasourceUrl::try_new(&self.base_url)
            .map_err(|_| ObjectStoreSourceError::Static("Couldn't parse base url"))?;
        let store = storage_options_into_object_store(&datasource_url, &self.storage_options)
            .map_err(|_| ObjectStoreSourceError::Static("Couldn't create a object store"))?;
        Ok(store)
    }

    fn path(&self, location: &str) -> Result<ObjectStorePath> {
        Ok(ObjectStorePath::from_url_path(location)?)
    }
}
