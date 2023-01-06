use datafusion::arrow::error::ArrowError;

#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreSourceError {
    #[error("Unknown fields for table")]
    UnknownFieldsForTable,

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error(transparent)]
    Parquet(#[from] datafusion::parquet::errors::ParquetError),

    #[error(transparent)]
    Arrow(#[from] ArrowError),

    #[error("Failed to decode json: {0}")]
    SerdeJson(#[from] serde_json::Error),
}

pub type Result<T, E = ObjectStoreSourceError> = std::result::Result<T, E>;

#[allow(clippy::from_over_into)]
impl Into<ArrowError> for ObjectStoreSourceError {
    fn into(self) -> ArrowError {
        ArrowError::ExternalError(Box::new(self))
    }
}
