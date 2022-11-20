#[derive(Debug, thiserror::Error)]
pub enum BackgroundError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = BackgroundError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::BackgroundError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
