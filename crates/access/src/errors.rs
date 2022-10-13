use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;

#[derive(Debug, thiserror::Error)]
pub enum AccessError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),

    #[error(transparent)]
    Parquet(#[from] ParquetError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = AccessError> = std::result::Result<T, E>;

#[allow(clippy::from_over_into)]
impl Into<ArrowError> for AccessError {
    fn into(self) -> ArrowError {
        ArrowError::ExternalError(Box::new(self))
    }
}

#[allow(clippy::from_over_into)]
impl Into<DataFusionError> for AccessError {
    fn into(self) -> DataFusionError {
        DataFusionError::External(Box::new(self))
    }
}

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::AccessError::Internal(std::format!($($arg)*))
    };
}
#[allow(unused_imports)]
pub(crate) use internal;
