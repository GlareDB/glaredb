pub mod scheduler;
pub mod stream;

mod executor;
mod pipeline;

#[derive(Debug, thiserror::Error)]
pub enum DistExecError {
    #[error("{0}")]
    String(String),
}

pub type Result<T, E = DistExecError> = std::result::Result<T, E>;
