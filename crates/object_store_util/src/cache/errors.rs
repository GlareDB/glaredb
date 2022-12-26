use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
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

    #[error("Cached file path already exists")]
    DuplicateCacheFile(PathBuf),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = CacheError> = std::result::Result<T, E>;

macro_rules! internal {
    ($($arg:tt)*) => {
        crate::cache::errors::CacheError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;

//TODO: Dynamically get cache name in the future
impl From<CacheError> for object_store::Error {
    fn from(e: CacheError) -> Self {
        match e {
            CacheError::ObjectStore(e) => e,
            e => Self::Generic {
                store: super::OBJECT_STORE_CACHE_NAME,
                source: Box::new(e),
            },
        }
    }
}
