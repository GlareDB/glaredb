pub mod executor;
pub mod pipeline;
pub mod scheduler;
pub mod stream;

mod adapter;
mod repartition;

#[derive(Debug, thiserror::Error)]
pub enum DistExecError {
    #[error("{0}")]
    String(String),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
}

pub type Result<T, E = DistExecError> = std::result::Result<T, E>;
