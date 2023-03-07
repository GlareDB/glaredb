use datafusion::arrow::error::ArrowError;

#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreSourceError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error(transparent)]
    Parquet(#[from] datafusion::parquet::errors::ParquetError),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("No file extension provided")]
    NoFileExtension,

    #[error("This file type is not supported: {0}")]
    NotSupportFileType(String),
}

pub type Result<T, E = ObjectStoreSourceError> = std::result::Result<T, E>;

impl From<ObjectStoreSourceError> for ArrowError {
    fn from(e: ObjectStoreSourceError) -> Self {
        ArrowError::ExternalError(Box::new(e))
    }
}
