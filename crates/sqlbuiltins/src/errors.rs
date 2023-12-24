use datafusion::arrow::error::ArrowError;

#[derive(Debug, thiserror::Error)]
pub enum BuiltinError {
    #[error(transparent)]
    DatafusionExtError(#[from] datafusion_ext::errors::ExtensionError),

    #[error(transparent)]
    DatafusionError(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    ArrowError(#[from] ArrowError),
}

pub type Result<T, E = BuiltinError> = std::result::Result<T, E>;
