use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion_ext::errors::ExtensionError;
use datasources::json::jaq::JaqError;

#[derive(Clone, Debug, thiserror::Error)]
pub enum BuiltinError {
    #[error("parse error: {0}")]
    ParseError(String),

    #[error("fundamental parsing error")]
    FundamentalError,

    #[error("missing value at index {0}")]
    MissingValueAtIndex(usize),

    #[error("expected value missing")]
    MissingValue,

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("columnar values not support at index {0}")]
    InvalidColumnarValue(usize),

    #[error("value was type {0}, expected {1}")]
    IncorrectType(DataType, DataType),

    #[error(transparent)]
    KdlError(#[from] kdl::KdlError),

    #[error("DataFusionError: {0}")]
    DataFusionError(String),

    #[error("ArrowError: {0}")]
    ArrowError(String),

    #[error("DataFusionExtension: {0}")]
    DataFusionExtension(String),

    #[error("serde_json: {0}")]
    SerdeJson(String),

    #[error(transparent)]
    BsonSer(#[from] bson::ser::Error),

    #[error("jaq_internal: {0}")]
    JaqInternal(String),

    #[error(transparent)]
    Jaq(#[from] JaqError),
}

pub type Result<T, E = BuiltinError> = std::result::Result<T, E>;

impl From<BuiltinError> for DataFusionError {
    fn from(e: BuiltinError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}

impl From<BuiltinError> for ExtensionError {
    fn from(e: BuiltinError) -> Self {
        ExtensionError::String(e.to_string())
    }
}

impl From<DataFusionError> for BuiltinError {
    fn from(e: DataFusionError) -> Self {
        BuiltinError::DataFusionError(e.to_string())
    }
}

impl From<ExtensionError> for BuiltinError {
    fn from(e: ExtensionError) -> Self {
        BuiltinError::DataFusionExtension(e.to_string())
    }
}

impl From<ArrowError> for BuiltinError {
    fn from(e: ArrowError) -> Self {
        BuiltinError::ArrowError(e.to_string())
    }
}

impl From<serde_json::Error> for BuiltinError {
    fn from(e: serde_json::Error) -> Self {
        BuiltinError::SerdeJson(e.to_string())
    }
}

impl From<jaq_interpret::Error> for BuiltinError {
    fn from(e: jaq_interpret::Error) -> Self {
        BuiltinError::JaqInternal(e.to_string())
    }
}
