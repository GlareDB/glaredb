use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct AnyClientCache {
    vtable: &'static AnyClientCacheVTable,
    cache: Arc<dyn Any + Sync + Send>,
}

#[derive(Debug)]
pub(crate) struct AnyClientCacheVTable {}

pub trait ClientCache {}
