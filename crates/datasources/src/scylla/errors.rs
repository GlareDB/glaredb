#[derive(Debug, thiserror::Error)]
pub enum ScyllaError {
    #[error("Unable to create Scylla session: {0}")]
    NewSessionError(#[from] scylla::transport::errors::NewSessionError),
    #[error("Unable to execute query: {0}")]
    QueryError(#[from] scylla::transport::errors::QueryError),
}

pub type Result<T, E = ScyllaError> = std::result::Result<T, E>;
