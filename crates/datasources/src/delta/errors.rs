#[derive(Debug, thiserror::Error)]
pub enum DeltaError {
    #[error(transparent)]
    DeltaTable(#[from] deltalake::DeltaTableError),

    #[error("Invalid table error from unity catalog: {error_code}: {message}")]
    UnityInvalidTable { error_code: String, message: String },

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    UrlParse(#[from] url::ParseError),

    #[error("{0}")]
    Static(&'static str),
}

pub type Result<T, E = DeltaError> = std::result::Result<T, E>;
