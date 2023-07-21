#[derive(Debug, thiserror::Error)]
pub enum IcebergError {
    #[error("{0}")]
    Static(&'static str),
}

pub type Result<T, E = IcebergError> = std::result::Result<T, E>;
