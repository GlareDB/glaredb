use access::deltacache::DeltaCache;
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
}

impl AccessRuntime {
    pub fn new() -> AccessRuntime {
        AccessRuntime {
            inner: Arc::new(Inner {
                conf: AccessConfig::default(),
                deltas: Arc::new(DeltaCache::new()),
            }),
        }
    }

    pub fn delta_cache(&self) -> &Arc<DeltaCache> {
        &self.inner.deltas
    }

    pub fn config(&self) -> &AccessConfig {
        &self.inner.conf
    }
}
