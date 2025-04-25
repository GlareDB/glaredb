use std::io;
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::{File, FileOpenContext, FileStat, FileSystem, OpenFlags};
use glaredb_error::Result;

#[derive(Debug)]
pub struct OriginFileHandle {}

impl File for OriginFileHandle {
    fn path(&self) -> &str {
        unimplemented!()
    }

    fn size(&self) -> usize {
        unimplemented!()
    }

    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        unimplemented!()
    }

    fn poll_write(&mut self, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        unimplemented!()
    }

    fn poll_seek(&mut self, cx: &mut Context, seek: io::SeekFrom) -> Poll<Result<()>> {
        unimplemented!()
    }

    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        unimplemented!()
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
