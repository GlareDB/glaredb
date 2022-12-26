//! Cached object store module.
mod errors;
mod store;

pub use errors::*;
pub use store::*;

/// Default size of cached ranges from the object file
pub const DEFAULT_BYTE_RANGE_SIZE: usize = 4096;
pub const OBJECT_STORE_CACHE_NAME: &str = "Object Storage Cache";
