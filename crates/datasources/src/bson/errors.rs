#[derive(Debug, thiserror::Error)]
pub enum BsonError {
    #[error("Unsupported bson type: {0}")]
    UnsupportedBsonType(&'static str),

    #[error("Unexpected datatype for builder {0:?}")]
    UnexpectedDataTypeForBuilder(datafusion::arrow::datatypes::DataType),

    #[error("Unhandled element type to arrow type conversion; {0:?}, {1}")]
    UnhandledElementType(
        bson::spec::ElementType,
        datafusion::arrow::datatypes::DataType,
    ),

    #[error("Invalid args for record struct builder")]
    InvalidArgsForRecordStructBuilder,

    #[error("Failed to read raw bson document")]
    FailedToReadRawBsonDocument,

    #[error("Column not in inferred schema: {0}")]
    ColumnNotInInferredSchema(String),

    #[error("Recursion limit exceeded for schema inferrence: {0}")]
    RecursionLimitExceeded(usize),
}

pub type Result<T, E = BsonError> = std::result::Result<T, E>;
