use datafusion::{arrow::datatypes::DataType, error::DataFusionError};

#[derive(Clone, Debug, thiserror::Error)]
pub enum BuiltinError {
    #[error("parse error: {0}")]
    ParseError(String),

    #[error("missing value at index {0}")]
    MissingValueAtIndex(usize),

    #[error("value at index {0} was {1}, expected {2}")]
    IncorrectTypeAtIndex(usize, DataType, DataType),

    #[error(transparent)]
    KdlError(#[from] kdl::KdlError),
}

pub type Result<T, E = BuiltinError> = std::result::Result<T, E>;

impl From<BuiltinError> for DataFusionError {
    fn from(e: BuiltinError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}
