#[derive(thiserror::Error, Debug)]
pub enum PushExecError {
    #[error("{0}")]
    Static(&'static str),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Failed to build runtime: {context}, error: {error}")]
    BuildRuntime {
        context: String,
        error: Box<dyn std::error::Error + Sync + Send>,
    },
}

pub type Result<T, E = PushExecError> = std::result::Result<T, E>;
