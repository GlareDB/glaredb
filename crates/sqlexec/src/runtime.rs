use access::deltacache::DeltaCache;
use object_store::{local::LocalFileSystem, ObjectStore};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct AccessConfig {
    pub trace_table_scan: bool,
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
    store: Arc<dyn ObjectStore>,
}

impl AccessRuntime {
    pub fn new(store: Arc<dyn ObjectStore>) -> AccessRuntime {
        AccessRuntime {
            inner: Arc::new(Inner {
                conf: AccessConfig::default(),
                deltas: Arc::new(DeltaCache::new()),
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

    pub fn config(&self) -> &AccessConfig {
        &self.inner.conf
    }
}

impl Default for AccessRuntime {
    fn default() -> Self {
        let local = LocalFileSystem::new();
        Self::new(Arc::new(local))
    }
}
