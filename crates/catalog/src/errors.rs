use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = CatalogError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::CatalogError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
