#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("missing catalog entry; type: {typ}, name: {name}")]
    MissingEntry { typ: &'static str, name: String },

    #[error("duplicate entry for name: {0}")]
    DuplicateEntryForName(String),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = CatalogError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::CatalogError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
