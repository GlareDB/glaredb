use rayexec_error::Result;
use std::fmt::Debug;
use std::fs::File;
use std::future::{self, Future};
use std::io::{Read, Seek, SeekFrom};

pub trait AsyncReadAt: Send + Debug {
    /// Read an exact range of bytes starting at `start` from the source into
    /// `buf`.
    ///
    /// An error should be returned if `buf` cannot be completely filled.
    fn read_at(&mut self, start: usize, buf: &mut [u8]) -> impl Future<Output = Result<()>> + Send;
}

/// Implementation of async reading on top of a file.
///
/// Note that we're not using tokio's async read+sync traits as the
/// implementation for files will attempt to spawn the read on a block thread.
impl AsyncReadAt for File {
    fn read_at(&mut self, start: usize, buf: &mut [u8]) -> impl Future<Output = Result<()>> + Send {
        let result = read_at_sync(self, start, buf);
        future::ready(result)
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
