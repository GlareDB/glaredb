#[derive(thiserror::Error, Debug)]
pub enum PushExecError {
    #[error("{0}")]
    Static(&'static str),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
}

pub type Result<T, E = PushExecError> = std::result::Result<T, E>;
