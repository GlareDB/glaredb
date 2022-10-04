use crate::errors::{internal, Result};
use crate::file::LocalCache;
use object_store::{gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, ObjectStore};
use std::path::Path;
use std::sync::Arc;
use tracing::{info, trace};

#[derive(Debug)]
pub struct DataStore {
    cache: Arc<LocalCache>,
}

impl DataStore {
    pub fn new<O, P>(store: O, path: P) -> DataStore
    where
        O: ObjectStore,
        P: AsRef<Path>,
    {
        let cache = Arc::new(LocalCache::new(store, path.as_ref().to_path_buf(), 0));
        DataStore { cache }
    }
}
