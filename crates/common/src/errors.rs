#[derive(Debug, thiserror::Error)]
pub enum CommonError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = CommonError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::ConfigError::Internal(std::format!($($arg)*))
    };
}

#[allow(unused_imports)]
pub(crate) use internal;
