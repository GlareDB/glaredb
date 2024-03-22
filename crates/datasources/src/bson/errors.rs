use datafusion::error::DataFusionError;
use datafusion_ext::errors::ExtensionError;

use crate::object_store::errors::ObjectStoreSourceError;

#[derive(Debug, thiserror::Error)]
pub enum BsonError {
    #[error("Unsupported bson type: {0}")]
    UnspportedType(&'static str),

    #[error("Unexpected datatype for builder {0:?}")]
    UnexpectedDataTypeForBuilder(datafusion::arrow::datatypes::DataType),

    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Raw(#[from] bson::raw::Error),

    #[error(transparent)]
    Serialization(#[from] bson::de::Error),

    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreSourceError),

    #[error("Unhandled element type to arrow type conversion; {0:?}, {1}")]
    UnhandledElementType(
        bson::spec::ElementType,
        datafusion::arrow::datatypes::DataType,
    ),

    #[error("Invalid args for record struct builder")]
    RecordStructBuilderInvalidArgs,

    #[error("Cannot construct RecordStructBuilder without columns")]
    RecordStructBuilderRequiresColumns,

    #[error("Failed to read raw bson document")]
    FailedToReadRawBsonDocument,

    #[error("Column not in inferred schema: {0}")]
    ColumnNotInInferredSchema(String),

    #[error("Recursion limit exceeded for schema inferrence: {0}")]
    RecursionLimitExceeded(usize),

    #[error("no objects found {0}")]
    NotFound(String),
}

impl From<BsonError> for DataFusionError {
    fn from(e: BsonError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}

impl From<BsonError> for ExtensionError {
    fn from(e: BsonError) -> Self {
        ExtensionError::String(e.to_string())
    }
}

pub type Result<T, E = BsonError> = std::result::Result<T, E>;
