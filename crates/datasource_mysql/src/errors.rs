#[derive(Debug, thiserror::Error)]
pub enum MysqlError {
    #[error("Unsupported Mysql - type: {0}, column: {1}")]
    UnsupportedMysqlType(u8, String),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Ssh(#[from] datasource_common::errors::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    #[error(transparent)]
    Mysql(#[from] mysql_async::Error),

    #[error(transparent)]
    ConnectionUrl(#[from] mysql_async::UrlError),

    // TODO Remove
    #[error("Coming soon! This feature is unimplemented")]
    Unimplemented,
}

pub type Result<T, E = MysqlError> = std::result::Result<T, E>;
