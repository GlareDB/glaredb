#[derive(Debug, thiserror::Error)]
pub enum MongoDbError {
    #[error("Failed to merge schemas: {0}")]
    FailedSchemaMerge(datafusion::arrow::error::ArrowError),

    #[error("Recursion limit exceeded for schema inferrence: {0}")]
    RecursionLimitExceeded(usize),

    #[error("Invalid protocol: {0}")]
    InvalidProtocol(String),

    #[error(transparent)]
    MongoDB(#[from] mongodb::error::Error),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Bson(#[from] crate::bson::errors::BsonError),

    #[error(transparent)]
    RawBson(#[from] mongodb::bson::raw::Error),
}

pub type Result<T, E = MongoDbError> = std::result::Result<T, E>;
