pub mod scheduler;

mod executor;
mod pipeline;

#[derive(Debug, thiserror::Error)]
pub enum DistExecError {}

pub type Result<T, E = DistExecError> = std::result::Result<T, E>;
