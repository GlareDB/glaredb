use crate::repr::InternalValue;
use std::io;

pub type Result<T, E = StorageError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// A primary key may contain multiple values from multiple columns. Hitting
    /// this error means we're attempting to access a non-existent column.
    #[error("missing value for pk at idx: {idx}")]
    MissingPkPart { idx: usize },
    /// A value lookup is not of the type we're looking for.
    #[error("unexpected internal value: {0:?}")]
    UnexpectedInternalValue(InternalValue),
    /// (De)serialization errors related to bincode.
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    /// IO errors
    #[error(transparent)]
    Io(#[from] io::Error),
    /// FFI errors for RocksDB.
    #[error(transparent)]
    Rocks(#[from] rocksdb::Error),
    #[error("internal: {0}")]
    Internal(String),
    /// Laziness. Anyhow is not used in this crate, but other crates that are
    /// depended upon do use it.
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

/// Create an internal error variant using string formatting.
#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::StorageError::Internal(std::format!($($arg)*))
    };
}
