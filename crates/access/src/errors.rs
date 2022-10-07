#[derive(Debug, thiserror::Error)]
pub enum AccessError {
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = AccessError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::AccessError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
