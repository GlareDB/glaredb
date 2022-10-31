#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    IntError(#[from] std::num::TryFromIntError),

    #[error("Cannot read from cached file")]
    RetryCacheRead,

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = PersistenceError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::PersistenceError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;

//TODO: Dynamically get cache name in the future
impl From<PersistenceError> for object_store::Error {
    fn from(e: PersistenceError) -> Self {
        use PersistenceError::*;
        match e {
            ObjectStore(e) => e,
            e => Self::Generic {
                store: crate::object_cache::OBJECT_STORE_CACHE_NAME,
                source: Box::new(e),
            },
        }
    }
}
