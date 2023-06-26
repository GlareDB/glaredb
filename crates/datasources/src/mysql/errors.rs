#[derive(Debug, thiserror::Error)]
pub enum MysqlError {
    #[error("Unsupported Mysql - type: {0}, column: {1}")]
    UnsupportedMysqlType(u8, String),

    #[error("Unable to convert mysql row value for column {0}: {1}, datatype: {2}")]
    UnsupportedArrowType(usize, String, datafusion::arrow::datatypes::DataType),

    #[error("Unsupported tunnel '{0}' for MySQL")]
    UnsupportedTunnel(String),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    #[error(transparent)]
    Mysql(#[from] mysql_async::Error),

    #[error(transparent)]
    MysqlFromValue(#[from] mysql_async::FromValueError),

    #[error(transparent)]
    ConnectionUrl(#[from] mysql_async::UrlError),

    #[error(transparent)]
    Common(#[from] crate::common::errors::DatasourceCommonError),

    #[error(transparent)]
    SshKey(#[from] crate::common::ssh::key::SshKeyError),
    #[error(transparent)]
    SshTunnel(#[from] crate::common::ssh::session::SshTunnelError),
}

pub type Result<T, E = MysqlError> = std::result::Result<T, E>;
