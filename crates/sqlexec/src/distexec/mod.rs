//! Distributed execution

pub mod coordinator;

#[derive(Debug, thiserror::Error)]
pub enum DistExecError {
    #[error("{0}")]
    String(String),
}

pub type Result<T, E = DistExecError> = std::result::Result<T, E>;
