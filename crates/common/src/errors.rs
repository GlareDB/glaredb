#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("internal: {0}")]
    Internal(String),
}

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::ConfigError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
