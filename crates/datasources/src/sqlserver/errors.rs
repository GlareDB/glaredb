#[derive(Debug, thiserror::Error)]
pub enum SqlServerError {
    #[error("{0}")]
    String(String),
    #[error(transparent)]
    Tiberius(#[from] tiberius::error::Error),
}

pub type Result<T, E = SqlServerError> = std::result::Result<T, E>;
