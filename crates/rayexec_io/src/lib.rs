pub mod http;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use http::HttpReader;
use rayexec_error::Result;
use std::fmt::Debug;
use std::fs::File;
use std::future;
use std::io::{Read, Seek, SeekFrom};

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

/// Implementation of async reading on top of a file.
///
/// Note that we're not using tokio's async read+sync traits as the
/// implementation for files will attempt to spawn the read on a block thread.
impl AsyncReader for File {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        let mut buf = vec![0; len];
        let result = read_at_sync(self, start, &mut buf);
        let bytes = Bytes::from(buf);
        future::ready(result.map(|_| bytes)).boxed()
    }
}

/// Helper for synchronously reading into a buffer.
fn read_at_sync<R>(mut reader: R, start: usize, buf: &mut [u8]) -> Result<()>
where
    R: Read + Seek,
{
    reader.seek(SeekFrom::Start(start as u64))?;
    reader.read_exact(buf)?;
    Ok(())
}
