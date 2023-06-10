#[derive(Debug, thiserror::Error)]
pub enum DeltaError {
    #[error(transparent)]
    DataCatalog(#[from] deltalake::DataCatalogError),

    #[error("Invalid table error from unity catalog: {error_code}: {message}")]
    UnityInvalidTable { error_code: String, message: String },

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error("{0}")]
    Static(&'static str),
}

pub type Result<T, E = DeltaError> = std::result::Result<T, E>;
