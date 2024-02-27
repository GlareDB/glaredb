use datafusion::arrow::error::ArrowError;
use datafusion_ext::errors::ExtensionError;

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

    #[error(transparent)]
    GlobPatternError(#[from] glob::PatternError),

    #[error(transparent)]
    GlobError(#[from] glob::GlobError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    DatasourceCommonError(#[from] crate::common::errors::DatasourceCommonError),

    #[error("No file extension provided")]
    NoFileExtension,

    #[error("This file type is not supported: {0}")]
    NotSupportFileType(String),

    #[error("{0}")]
    InvalidHttpStatus(String),

    #[error("{0}")]
    Static(&'static str),

    #[error("{0}")]
    String(String),

    #[error("Failed to read object over http: {0}")]
    Reqwest(#[from] reqwest::Error),
}

pub type Result<T, E = ObjectStoreSourceError> = std::result::Result<T, E>;

impl From<ObjectStoreSourceError> for ArrowError {
    fn from(e: ObjectStoreSourceError) -> Self {
        ArrowError::ExternalError(Box::new(e))
    }
}

impl From<ObjectStoreSourceError> for ExtensionError {
    fn from(e: ObjectStoreSourceError) -> Self {
        ExtensionError::ObjectStore(e.to_string())
    }
}
