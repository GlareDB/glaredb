use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum DatasourceCommonError {
    #[error("Invalid SSH connection string: {0}")]
    SshConnectionParseError(String),

    #[error("Feature currently unsupported: {0}")]
    Unsupported(&'static str),

    #[error("Scalar of type '{0}' not supported")]
    UnsupportedDatafusionScalar(datafusion::arrow::datatypes::DataType),

    #[error("Invalid url: {0}")]
    InvalidUrl(String),

    #[error(transparent)]
    ReprError(#[from] repr::error::ReprError),

    #[error(transparent)]
    FmtError(#[from] core::fmt::Error),

    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),

    #[error(transparent)]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    DatafusionError(#[from] datafusion::common::DataFusionError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),
}

pub type Result<T, E = DatasourceCommonError> = std::result::Result<T, E>;


impl From<DatasourceCommonError> for DataFusionError {
    fn from(value: DatasourceCommonError) -> DataFusionError {
        DataFusionError::External(Box::new(value))
    }
}
