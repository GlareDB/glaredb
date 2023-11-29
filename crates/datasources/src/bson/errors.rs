#[derive(Debug, thiserror::Error)]
pub enum BsonError {
    #[error("Unsupported bson type: {0}")]
    UnsupportedBsonType(&'static str),

    #[error("Recursion limit exceeded for schema inferrence: {0}")]
    RecursionLimitExceeded(usize),
}

pub type Result<T, E = BsonError> = std::result::Result<T, E>;
