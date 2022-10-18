use access::compact::Compactor;
use access::deltacache::DeltaCache;
use object_store::ObjectStore;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct AccessConfig {}

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
    // TODO: Create the runtime from configuration.
    pub fn new(store: Arc<dyn ObjectStore>) -> AccessRuntime {
        AccessRuntime {
            inner: Arc::new(Inner {
                conf: AccessConfig::default(),
                deltas: Arc::new(DeltaCache::new()),
                compactor: Arc::new(Compactor::new(store.clone())),
                store,
            }),
        }
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
