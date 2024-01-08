#[derive(Debug, thiserror::Error)]
pub enum CassandraError {
    #[error("Unable to create Scylla session: {0}")]
    NewSessionError(#[from] scylla::transport::errors::NewSessionError),
    #[error("Unable to execute query: {0}")]
    QueryError(#[from] scylla::transport::errors::QueryError),
    #[error("Unsupported DataType: {0}")]
    UnsupportedDataType(String),
    #[error("{0}")]
    String(String),
}

pub type Result<T, E = CassandraError> = std::result::Result<T, E>;
