#[derive(Debug, thiserror::Error)]
pub enum RemoteExecError {
    #[error("Cluster error: {0}")]
    Tonic(#[from] tonic::Status),

    #[error("Error from GlareDB Cloud: {0}")]
    CloudResponse(String),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
}

pub type Result<T, E = RemoteExecError> = std::result::Result<T, E>;

impl From<RemoteExecError> for tonic::Status {
    fn from(value: RemoteExecError) -> Self {
        tonic::Status::from_error(Box::new(value))
    }
}
