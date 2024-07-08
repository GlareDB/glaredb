use std::fs::{File, OpenOptions};
use std::path::Path;

use bytes::Bytes;
use futures::future::{self, BoxFuture, FutureExt};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_io::{
    filesystem::{FileReader, FileSystemProvider},
    AsyncReader,
};
use std::io::{Read, Seek, SeekFrom};

/// Standard file system access, nothing special.
#[derive(Debug, Clone, Copy)]
pub struct LocalFileSystemProvider;

impl FileSystemProvider for LocalFileSystemProvider {
    fn reader(&self, path: &Path) -> Result<Box<dyn FileReader>> {
        let file = OpenOptions::new().read(true).open(path).map_err(|e| {
            RayexecError::with_source(
                format!(
                    "Failed to open file at location: {}",
                    path.to_string_lossy()
                ),
                Box::new(e),
            )
        })?;

        Ok(Box::new(LocalFile { file }))
    }
}

#[derive(Debug)]
pub struct LocalFile {
    file: File,
}

impl FileReader for LocalFile {
    fn size(&mut self) -> BoxFuture<Result<usize>> {
        let result = self.file.metadata();
        async move { Ok(result.context("failed to get file metadata")?.len() as usize) }.boxed()
    }
}

/// Implementation of async reading on top of a file.
///
/// Note that we're not using tokio's async read+sync traits as the
/// implementation for files will attempt to spawn the read on a block thread.
impl AsyncReader for LocalFile {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        let mut buf = vec![0; len];
        let result = read_at(&mut self.file, start, &mut buf);
        let bytes = Bytes::from(buf);
        future::ready(result.map(|_| bytes)).boxed()
    }
}

/// Helper for synchronously reading into a buffer.
fn read_at<R>(mut reader: R, start: usize, buf: &mut [u8]) -> Result<()>
where
    R: Read + Seek,
{
    reader.seek(SeekFrom::Start(start as u64))?;
    reader.read_exact(buf)?;
    Ok(())
}
