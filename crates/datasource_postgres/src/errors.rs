#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    #[error("Unsupported Postgres type: {0}")]
    UnsupportedPostgresType(String),

    #[error("Unknown Postgres OIDs: {0:?}")]
    UnknownPostgresOids(Vec<u32>),

    #[error("Provide one Postgres host to connect: {0:?}")]
    IncorrectNumberOfHosts(Vec<tokio_postgres::config::Host>),

    #[error("Too many ports provided. Provide one port or no ports (default port 5432 will be used): {0:?}")]
    TooManyPorts(Vec<u16>),

    #[error("Unable to copy binary row value for datatype: {0}")]
    FailedBinaryCopy(datafusion::arrow::datatypes::DataType),

    #[error("Failed to connect to Postgres instance: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Ssh(#[from] datasource_common::errors::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),
}

pub type Result<T, E = PostgresError> = std::result::Result<T, E>;
