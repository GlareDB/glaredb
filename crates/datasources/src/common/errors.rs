#[derive(Debug, thiserror::Error)]
pub enum DatasourceCommonError {
    #[error("Invalid SSH connection string: {0}")]
    SshConnectionParseError(String),

    #[error("Feature currently unsupported: {0}")]
    Unsupported(&'static str),

    #[error(transparent)]
    ListingErrBoxed(#[from] Box<dyn std::error::Error + Sync + Send>),

    #[error("Scalar of type '{0}' not supported")]
    UnsupportedDatafusionScalar(datafusion::arrow::datatypes::DataType),

    #[error(transparent)]
    ReprError(#[from] repr::error::ReprError),

    #[error(transparent)]
    FmtError(#[from] core::fmt::Error),
}

pub type Result<T, E = DatasourceCommonError> = std::result::Result<T, E>;
