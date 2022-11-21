#[derive(Debug, thiserror::Error)]
pub enum BackgroundError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    Cloud(#[from] cloud::errors::CloudError),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = BackgroundError> = std::result::Result<T, E>;
