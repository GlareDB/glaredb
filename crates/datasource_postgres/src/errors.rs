#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    #[error("Unsupported Postgres type: {0}")]
    UnsupportedPostgresType(String),

    #[error("Unknown Postgres OIDs: {0:?}")]
    UnknownPostgresOids(Vec<u32>),

    #[error("Unable to copy binary row value for datatype: {0}")]
    FailedBinaryCopy(datafusion::arrow::datatypes::DataType),

    #[error("Failed to connect to Postgres instance: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),
}

pub type Result<T, E = PostgresError> = std::result::Result<T, E>;
