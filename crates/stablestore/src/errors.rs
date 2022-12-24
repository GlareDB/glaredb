#[derive(Debug, thiserror::Error)]
pub enum StableStorageError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = StableStorageError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::StableStorageError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
