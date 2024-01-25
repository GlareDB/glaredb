pub type Result<T, E = RayExecError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum RayExecError {
    #[error("{0}")]
    String(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

impl RayExecError {
    pub fn new(msg: impl Into<String>) -> Self {
        RayExecError::String(msg.into())
    }
}

pub fn err(msg: impl Into<String>) -> RayExecError {
    RayExecError::new(msg)
}
