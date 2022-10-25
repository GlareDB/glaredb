use crate::errors::Result;
use access::compact::Compactor;
use access::deltacache::DeltaCache;
use object_store::ObjectStore;
use object_store_util::temp::TempObjectStore;
use persistence::object_cache::{ObjectStoreCache, DEFAULT_BYTE_RANGE_SIZE};
use std::{path::PathBuf, sync::Arc};

#[derive(Debug, Default)]
pub enum ObjectStoreKind {
    #[default]
    LocalTemp,
    //TODO: add data needed to create gcp ObjectStore
    Gcp,
}

#[derive(Debug)]
pub struct AccessConfig {
    pub object_store: ObjectStoreKind,
    pub cached: bool,
    pub max_object_store_cache_size: Option<u64>,
    pub cache_path: Option<PathBuf>,
}

impl Default for AccessConfig {
    fn default() -> Self {
        Self {
            object_store: Default::default(),
            cached: false,
            max_object_store_cache_size: None,
            cache_path: None,
        }
    }
}

/// Global resources for accessing data.
#[derive(Debug, Clone)]
pub struct AccessRuntime {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    conf: AccessConfig,
    deltas: Arc<DeltaCache>,
    compactor: Arc<Compactor>,
    store: Arc<dyn ObjectStore>,
}

impl AccessRuntime {
    /// Create a new access runtime with the given object store.
    pub fn new(config: AccessConfig) -> Result<AccessRuntime> {
        use ObjectStoreKind::*;
        let mut store = match config.object_store {
            LocalTemp => Arc::new(TempObjectStore::new()?) as Arc<dyn ObjectStore>,
            Gcp => todo!(),
        };

        if config.cached {
            store = Arc::new(ObjectStoreCache::new(
                config
                    .cache_path
                    .as_ref()
                    .expect("No cache path provided")
                    .as_path(),
                DEFAULT_BYTE_RANGE_SIZE,
                config
                    .max_object_store_cache_size
                    .expect("no max cache size provided"),
                store,
            )?);
        }

        Ok(AccessRuntime {
            inner: Arc::new(Inner {
                conf: config,
                deltas: Arc::new(DeltaCache::new()),
                compactor: Arc::new(Compactor::new(store.clone())),
                store,
            }),
        })
    }

    pub fn delta_cache(&self) -> &Arc<DeltaCache> {
        &self.inner.deltas
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.inner.store
    }

    pub fn compactor(&self) -> &Arc<Compactor> {
        &self.inner.compactor
    }

    pub fn config(&self) -> &AccessConfig {
        &self.inner.conf
    }
}
