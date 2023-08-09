#[derive(Debug, thiserror::Error)]
pub enum RpcsrvError {
    #[error("Invalid {0} id: {1}")]
    InvalidId(&'static str, uuid::Error),

    #[error("Missing session for id {0}")]
    MissingSession(uuid::Uuid),

    #[error("Executing physical plans is not currently supported")]
    PhysicalPlansNotSupported,

    #[error("Missing key: {0}")]
    MissingAuthKey(&'static str),

    #[error("Session initialize error: {0}")]
    SessionInitalizeError(String),

    #[error(transparent)]
    TonicMetadataToStr(#[from] tonic::metadata::errors::ToStrError),

    #[error(transparent)]
    ProtoConvError(#[from] protogen::metastore::types::ProtoConvError),

    #[error(transparent)]
    ExecError(#[from] sqlexec::errors::ExecError),

    #[error("Failed to authenticate with GlareDB Cloud: {0}")]
    CloudAuth(#[from] proxyutil::cloudauth::CloudAuthError),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("{0:?}")]
    TonicTransport(#[from] tonic::transport::Error),

    #[error(transparent)]
    InvalidMetadataValue(#[from] tonic::metadata::errors::InvalidMetadataValue),

    #[error("{0}")]
    Internal(String),
}

pub type Result<T, E = RpcsrvError> = std::result::Result<T, E>;

impl From<RpcsrvError> for tonic::Status {
    fn from(value: RpcsrvError) -> Self {
        tonic::Status::from_error(Box::new(value))
    }
}
