pub mod http;

use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream};
use rayexec_error::Result;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Debug},
    path::PathBuf,
};
use url::Url;

/// Location for a file.
// TODO: Glob/hive
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileLocation {
    Url(Url),
    Path(PathBuf),
}

impl FileLocation {
    /// Parse a file location from a string.
    ///
    /// Current implementation assumes that if the string fails to parse as a
    /// url, it must be a path. However further checks will need to be done when
    /// we support globs and hive partitioning here.
    pub fn parse(s: &str) -> Self {
        match Url::parse(s) {
            Ok(url) => FileLocation::Url(url),
            Err(_) => FileLocation::Path(PathBuf::from(s)),
        }
    }
}

impl fmt::Display for FileLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Url(u) => write!(f, "{}", u),
            Self::Path(p) => write!(f, "{}", p.display()),
        }
    }
}

/// Provide file sources and sinks.
///
/// Different source/sink implementations can be returned depending on the file
/// location variant (url or path). If this file provider cannot handle a specific
/// variant, this should just return an error.
// TODO: List/delete. With globs and hive, we could do something fancy where we
// have an async stream of sources, but idk if that's good idea yet.
pub trait FileProvider: Sync + Send + Debug {
    /// Gets a file source at some location.
    fn file_source(&self, location: FileLocation) -> Result<Box<dyn FileSource>>;

    /// Gets a file sink at some location
    fn file_sink(&self, location: FileLocation) -> Result<Box<dyn FileSink>>;
}

/// Asynchronous reads of some file source.
pub trait FileSource: Sync + Send + Debug {
    /// Read a complete range of bytes.
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>>;

    /// Stream bytes from a source.
    // TODO: Change to `into_read_stream`
    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>>;

    /// Get the size in bytes for a file.
    ///
    /// For data sources like parquet files, this is necessary as we need to be
    /// able to read the metadata at the end of a file to allow us to only fetch
    /// byte ranges.
    ///
    /// For other data sources like json and csv, this can be skipped and the
    /// content can just be streamed.
    fn size(&mut self) -> BoxFuture<Result<usize>>;
}

// TODO: Possibly remove this and just use boxed trait where needed. Will likely
// need to be done if want to change `read_stream` to `into_read_stream`.
impl FileSource for Box<dyn FileSource + '_> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        self.as_mut().read_range(start, len)
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        self.as_mut().read_stream()
    }

    fn size(&mut self) -> BoxFuture<Result<usize>> {
        self.as_mut().size()
    }
}

/// Asynchronous writes to some file source.
///
/// The semantics for this is overwrite any existing data. If appending is
/// needed, a separate trait should be created.
pub trait FileSink: Sync + Send + Debug {
    /// Write all bytes.
    fn write_all(&mut self, buf: Bytes) -> BoxFuture<'static, Result<()>>;

    /// Finish the write, including flushing out any pending bytes.
    ///
    /// This should be called after _all_ data has been written.
    fn finish(&mut self) -> BoxFuture<'static, Result<()>>;
}

impl FileSink for Box<dyn FileSink + '_> {
    fn write_all(&mut self, buf: Bytes) -> BoxFuture<'static, Result<()>> {
        self.as_mut().write_all(buf)
    }

    fn finish(&mut self) -> BoxFuture<'static, Result<()>> {
        self.as_mut().finish()
    }
}
