#[derive(Debug, thiserror::Error)]
pub enum CloudError {
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),

    #[error("cloud communication disabled")]
    CloudCommsDisabled,

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = CloudError> = std::result::Result<T, E>;
