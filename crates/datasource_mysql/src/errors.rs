#[derive(Debug, thiserror::Error)]
pub enum MysqlError {
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Ssh(#[from] datasource_common::errors::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    // TODO Remove
    #[error("Coming soon! This feature is unimplemented")]
    Unimplemented,
}

pub type Result<T, E = MysqlError> = std::result::Result<T, E>;
