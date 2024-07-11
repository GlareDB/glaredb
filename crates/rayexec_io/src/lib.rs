pub mod filesystem;
pub mod http;

use bytes::Bytes;
use filesystem::FileReader;
use futures::{future::BoxFuture, stream::BoxStream};
use rayexec_error::Result;
use std::fmt::Debug;

pub trait AsyncReader: Sync + Send + Debug {
    /// Read a complete range of bytes.
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>>;

    /// Stream bytes from a source.
    // TODO: Change to `into_read_stream`
    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>>;
}

pub trait FileSource: AsyncReader {
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

impl AsyncReader for Box<dyn FileSource + '_> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        self.as_mut().read_range(start, len)
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        self.as_mut().read_stream()
    }
}

impl AsyncReader for Box<dyn AsyncReader + '_> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        self.as_mut().read_range(start, len)
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        self.as_mut().read_stream()
    }
}

impl AsyncReader for Box<dyn FileReader + '_> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        self.as_mut().read_range(start, len)
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        self.as_mut().read_stream()
    }
}
