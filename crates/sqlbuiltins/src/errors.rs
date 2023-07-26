#[derive(Debug, thiserror::Error)]
pub enum BuiltinError {
    #[error(transparent)]
    DatafusionExtError(#[from] datafusion_ext::errors::ExtensionError),
}

pub type Result<T, E = BuiltinError> = std::result::Result<T, E>;
