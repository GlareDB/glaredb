#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("channel disconnected")]
    Disconnected,

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

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
