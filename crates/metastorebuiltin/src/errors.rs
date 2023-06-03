#[derive(Debug, thiserror::Error)]
pub enum BuiltinError {
    #[error("Failed to create builtin catalog: {0}")]
    CreateBuiltinCatalog(String),
}

pub type Result<T, E = BuiltinError> = std::result::Result<T, E>;
