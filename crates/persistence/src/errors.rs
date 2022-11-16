use snafu::prelude::*;
use std::path::PathBuf;

#[derive(Debug, Snafu)]
pub enum PersistenceError {
    #[snafu(context(false))]
    ObjectStore {
        source: object_store::Error,
        backtrace: snafu::Backtrace,
    },

    #[snafu(context(false))]
    ObjectStorePath {
        source: object_store::path::Error,
        backtrace: snafu::Backtrace,
    },

    #[snafu(context(false))]
    Io {
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },

    #[snafu(display("Error length {length}, source: {source}"))]
    ContentsTooLarge {
        source: std::num::TryFromIntError,
        length: usize,
    },

    #[snafu(display("Error: Retry read from disk cache"))]
    RetryCacheRead,

    #[snafu(display("Duplicate cache file generated at {}", path.display()))]
    DuplicateCacheFile { path: PathBuf },

    #[snafu(display("Error: {msg}"))]
    Internal { msg: String },
}

pub type Result<T, E = PersistenceError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::PersistenceError::Internal{ msg: std::format!($($arg)*) }
    };
}

pub(crate) use internal;

//TODO: Dynamically get cache name in the future
impl From<PersistenceError> for object_store::Error {
    fn from(e: PersistenceError) -> Self {
        use PersistenceError::*;
        match e {
            ObjectStore { source, .. } => source,
            e => Self::Generic {
                store: crate::object_cache::OBJECT_STORE_CACHE_NAME,
                source: Box::new(e),
            },
        }
    }
}
