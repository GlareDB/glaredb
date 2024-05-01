#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    #[error("Failed to query Postgres: {0}")]
    QueryError(String),

    #[error("Unsupported Postgres type: {0}")]
    UnsupportedPostgresType(String),

    #[error("Unknown Postgres OIDs: {0:?}")]
    UnknownPostgresOids(Vec<u32>),

    #[error("No valid postgres host found to connect: {0:?}")]
    InvalidPgHosts(Vec<tokio_postgres::config::Host>),

    #[error("Too many ports provided. Provide one port or no ports (default port 5432 will be used): {0:?}")]
    TooManyPorts(Vec<u16>),

    #[error("Unable to copy binary row value for datatype: {0}")]
    FailedBinaryCopy(datafusion::arrow::datatypes::DataType),

    #[error("Failed to connect to Postgres instance: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),

    #[error("Unuspported ssl mode: {0:?}")]
    UnsupportSslMode(tokio_postgres::config::SslMode),

    #[error("Unsupported tunnel '{0}' for Postgres")]
    UnsupportedTunnel(String),

    #[error("Overflow converting '{0}' to {1}")]
    DataOverflow(String, datafusion::arrow::datatypes::DataType),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    #[error(transparent)]
    TryFromIntError(#[from] std::num::TryFromIntError),

    #[error(transparent)]
    DecimalError(#[from] decimal::DecimalError),

    #[error(transparent)]
    Common(#[from] crate::common::errors::DatasourceCommonError),

    #[error(transparent)]
    ProtoConv(#[from] protogen::ProtoConvError),

    #[error(transparent)]
    SshKey(#[from] crate::common::ssh::key::SshKeyError),
    #[error(transparent)]
    SshTunnel(#[from] crate::common::ssh::session::SshTunnelError),

    #[error(transparent)]
    InvalidDnsName(#[from] rustls::pki_types::InvalidDnsNameError),
}

pub type Result<T, E = PostgresError> = std::result::Result<T, E>;
