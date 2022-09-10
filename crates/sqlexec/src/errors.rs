#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    DataFusion(#[from] datafusion::common::DataFusionError),
}

pub type Result<T, E = ExecError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::ExecError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
