//! Coordinator for distributed execution

pub mod handler;
pub mod scheduler;

#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error("{0}")]
    String(String),
}

pub type Result<T, E = CoordinatorError> = std::result::Result<T, E>;
