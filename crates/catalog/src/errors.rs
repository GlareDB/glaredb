#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("missing system table: {0}")]
    MissingSystemTable(String),

    #[error("table read-only")]
    TableReadonly,

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
