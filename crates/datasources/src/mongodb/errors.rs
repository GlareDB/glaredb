#[derive(Debug, thiserror::Error)]
pub enum MongoError {
    #[error("Unsupported bson type: {0}")]
    UnsupportedBsonType(&'static str),

    #[error("Failed to merge schemas: {0}")]
    FailedSchemaMerge(datafusion::arrow::error::ArrowError),

    #[error("Failed to read raw bson document")]
    FailedToReadRawBsonDocument,

    #[error("Column not in inferred schema: {0}")]
    ColumnNotInInferredSchema(String),

    #[error("Unexpected datatype for builder {0:?}")]
    UnexpectedDataTypeForBuilder(datafusion::arrow::datatypes::DataType),

    #[error("Recursion limit exceeded for schema inferrence: {0}")]
    RecursionLimitExceeded(usize),

    #[error("Invalid args for record struct builder")]
    InvalidArgsForRecordStructBuilder,

    #[error("Unhandled element type to arrow type conversion; {0:?}, {1}")]
    UnhandledElementType(
        mongodb::bson::spec::ElementType,
        datafusion::arrow::datatypes::DataType,
    ),

    #[error("Invalid protocol: {0}")]
    InvalidProtocol(String),

    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
}

pub type Result<T, E = MongoError> = std::result::Result<T, E>;
