use access::compact::Compactor;
use access::deltacache::DeltaCache;
use object_store::{local::LocalFileSystem, ObjectStore};
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

// TODO: Remove this.
impl Default for AccessRuntime {
    fn default() -> Self {
        let tmp = std::env::temp_dir().join("access_runtime");
        std::fs::create_dir_all(&tmp).unwrap();
        let local = LocalFileSystem::new_with_prefix(tmp).unwrap();
        Self::new(Arc::new(local))
    }
}
