#[derive(Debug, thiserror::Error)]
pub enum DebugError {
    #[error("Unknown debug table type: {0}")]
    UnknownDebugTableType(String),
    #[error("Execution error: {0}")]
    ExecutionError(&'static str),
    #[error("Invalid tunnel for Debug datasource: {0}")]
    InvalidTunnel(String),
}

pub type Result<T, E = DebugError> = std::result::Result<T, E>;
