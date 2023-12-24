use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;

#[derive(Clone, Debug, thiserror::Error)]
pub enum BuiltinError {
    #[error("parse error: {0}")]
    ParseError(String),

    #[error("missing value at index {0}")]
    MissingValueAtIndex(usize),

    #[error("invalid value at index {0}")]
    InvalidValueAtIndex(usize, String),

    #[error("value at index {0} was {1}, expected {2}")]
    IncorrectTypeAtIndex(usize, DataType, DataType),

    #[error(transparent)]
    KdlError(#[from] kdl::KdlError),

    #[error("DataFusionError: {0}")]
    DataFusionError(String),

    #[error("ArrowError: {0}")]
    ArrowError(String),
}

pub type Result<T, E = BuiltinError> = std::result::Result<T, E>;

impl From<BuiltinError> for DataFusionError {
    fn from(e: BuiltinError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}

impl From<DataFusionError> for BuiltinError {
    fn from(e: DataFusionError) -> Self {
        BuiltinError::DataFusionError(e.to_string())
    }
}

impl From<ArrowError> for BuiltinError {
    fn from(e: ArrowError) -> Self {
        BuiltinError::ArrowError(e.to_string())
    }
}
