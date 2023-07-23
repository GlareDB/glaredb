#[derive(Debug, thiserror::Error)]
pub enum IcebergError {
    #[error("Data is invalid: {0}")]
    DataInvalid(String),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("{0}")]
    Static(&'static str),
}

pub type Result<T, E = IcebergError> = std::result::Result<T, E>;
