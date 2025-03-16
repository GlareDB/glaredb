pub mod access;
pub mod exp;
pub mod future;
pub mod http;
pub mod location;
pub mod memory;
pub mod s3;

mod util;

use std::fmt::Debug;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use location::{AccessConfig, FileLocation};
use glaredb_error::Result;

/// Provide file sources and sinks.
///
/// Different source/sink implementations can be returned depending on the file
/// location variant (url or path). If this file provider cannot handle a specific
/// variant, this should just return an error.
// TODO: List/delete. With globs and hive, we could do something fancy where we
// have an async stream of sources, but idk if that's good idea yet.
pub trait FileProvider2: Sync + Send + Debug {
    /// Gets a file source at some location.
    fn file_source(
        &self,
        location: FileLocation,
        config: &AccessConfig,
    ) -> Result<Box<dyn FileSource>>;

    /// Gets a file sink at some location
    fn file_sink(&self, location: FileLocation, config: &AccessConfig)
        -> Result<Box<dyn FileSink>>;

    /// Return a stream of paths relative to `prefix`.
    ///
    /// This is stream of vecs to allow for easily adapting to object store
    /// pagination.
    ///
    /// The relative paths returned should be for "objects". Specifically for
    /// the filesystem implementation, directory paths should not be returned,
    /// only paths to a file.
    ///
    /// Paths should be returned lexicographically ascending order.
    fn list_prefix(
        &self,
        prefix: FileLocation,
        config: &AccessConfig,
    ) -> BoxStream<'static, Result<Vec<String>>>;
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

/// Extension traits that provide convenience utilities.
///
/// Default implementations can be overridden for increased efficiency.
pub trait FileSourceExt: FileSource {
    /// Creates a stream from the source and reads it to completion.
    ///
    /// The returned bytes is the complete response.
    fn read_stream_all(&mut self) -> BoxFuture<'static, Result<Bytes>> {
        let mut stream = self.read_stream();
        Box::pin(async move {
            let mut buf = Vec::new();

            while let Some(result) = stream.next().await {
                let bs = result?;
                buf.extend_from_slice(bs.as_ref());
            }

            Ok(buf.into())
        })
    }
}

impl<S: FileSource> FileSourceExt for S {}

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
