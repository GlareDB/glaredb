use std::io;
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::{File, FileOpenContext, FileStat, FileSystem, OpenFlags};
use glaredb_error::{DbError, Result};
use web_sys::{FileSystemFileHandle, FileSystemSyncAccessHandle};

#[derive(Debug)]
pub struct OriginFileHandle {
    /// Path to the file.
    path: String,
    /// Size in bytes of the file.
    len: usize,
    /// Current position within the file.
    pos: usize,
    /// Web sys handle to the file.
    handle: FileSystemSyncAccessHandle,
}

// Sync makes sense, I'm not sure why websys doesn't make it sync automatically.
//
// Send is something we should probably think about a bit more, and maybe have
// the Send restriction optional for the pipeline/system runtime.
//
// But right now, this is fine. We can't actually send it anywhere.
unsafe impl Sync for OriginFileHandle {}
unsafe impl Send for OriginFileHandle {}

impl File for OriginFileHandle {
    fn path(&self) -> &str {
        &self.path
    }

    fn size(&self) -> usize {
        self.len
    }

    fn poll_read(&mut self, _cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        // TODO: Loses error context.
        let n = self
            .handle
            .read_with_u8_array(buf)
            .map_err(|_val| DbError::new("Failed to read from file"))?;
        let n = n as usize;

        self.pos += n;

        Poll::Ready(Ok(n))
    }

    fn poll_write(&mut self, _cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        unimplemented!()
    }

    fn poll_seek(&mut self, _cx: &mut Context, seek: io::SeekFrom) -> Poll<Result<()>> {
        match seek {
            io::SeekFrom::Start(count) => self.pos = count as usize,
            io::SeekFrom::End(count) => {
                if count > 0 {
                    // It's legal to seek beyond the end, but the read may fail.
                    self.pos = self.len + (count as usize);
                } else {
                    let count = count.abs();
                    if count as usize > self.len {
                        return Poll::Ready(Err(DbError::new(
                            "Cannot seek to before beginning of file",
                        )));
                    }
                    self.pos = self.len - (count as usize);
                }
            }
            io::SeekFrom::Current(count) => {
                if count > 0 {
                    // Just add to current position, as above, it's legal to seek beyond the end.
                    self.pos += count as usize;
                } else {
                    let count = count.unsigned_abs() as usize;
                    if count > self.pos {
                        return Poll::Ready(Err(DbError::new(
                            "Cannot seek to before beginning of file",
                        )));
                    }
                    self.pos -= count;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        // TOOD: Loses error context.
        self.handle
            .flush()
            .map_err(|_| DbError::new("Failed to flush file"))?;
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub struct OriginFileSystem {}

impl FileSystem for OriginFileSystem {
    type File = OriginFileHandle;
    type State = ();

    fn state_from_context(&self, context: FileOpenContext) -> Result<Self::State> {
        unimplemented!()
    }

    async fn open(&self, flags: OpenFlags, path: &str, state: &Self::State) -> Result<Self::File> {
        unimplemented!()
    }

    async fn stat(&self, path: &str, state: &Self::State) -> Result<Option<FileStat>> {
        unimplemented!()
    }

    fn can_handle_path(&self, path: &str) -> bool {
        unimplemented!()
    }
}
