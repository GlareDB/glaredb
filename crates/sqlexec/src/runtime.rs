use access::deltacache::DeltaCache;
use std::sync::Arc;

/// Global resources for accessing data.
#[derive(Debug, Clone)]
pub struct AccessRuntime {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    deltas: Arc<DeltaCache>,
}

impl AccessRuntime {
    pub fn new() -> AccessRuntime {
        AccessRuntime {
            inner: Arc::new(Inner {
                deltas: Arc::new(DeltaCache::new()),
            }),
        }
    }

    pub fn delta_cache(&self) -> &Arc<DeltaCache> {
        &self.inner.deltas
    }
}
