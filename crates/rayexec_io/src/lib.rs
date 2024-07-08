pub mod filesystem;
pub mod http;

use bytes::Bytes;
use filesystem::FileReader;
use futures::future::BoxFuture;
use http::HttpReader;
use rayexec_error::Result;
use std::fmt::Debug;

pub trait AsyncReader: Sync + Send + Debug {
    /// Read a complete range of bytes.
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>>;
}

impl AsyncReader for Box<dyn AsyncReader + '_> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        self.as_mut().read_range(start, len)
    }
}

impl AsyncReader for Box<dyn HttpReader + '_> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        self.as_mut().read_range(start, len)
    }
}

impl AsyncReader for Box<dyn FileReader + '_> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        self.as_mut().read_range(start, len)
    }
}
