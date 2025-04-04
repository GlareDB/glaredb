pub mod dispatch;

use std::any::Any;
use std::fmt::Debug;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use glaredb_error::Result;

pub trait File: Debug + Sync + Send + 'static {
    /// Get the size in bytes of this file.
    fn size(&self) -> usize;

    /// Read from the current position into the buffer, returning the number of
    /// bytes read.
    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>>;

    /// Write at the current position, returning the number of bytes written.
    fn poll_write(&mut self, buf: &mut [u8]) -> Poll<Result<usize>>;

    /// Seek the provided position in the file.
    fn poll_seek(&mut self, seek: io::SeekFrom) -> Poll<Result<()>>;

    /// Flush the file to disk (or persistent storage).
    fn poll_flush(&mut self) -> Poll<Result<()>>;
}

#[derive(Debug)]
pub struct AnyFile {
    pub(crate) vtable: &'static RawFileVTable,
    pub(crate) file: Box<dyn Any + Sync + Send>,
}

impl AnyFile {
    pub fn from_file<F>(file: F) -> Self
    where
        F: File,
    {
        AnyFile {
            vtable: F::VTABLE,
            file: Box::new(file),
        }
    }

    pub fn call_poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        (self.vtable.poll_read_fn)(self.file.as_mut(), cx, buf)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawFileVTable {
    poll_read_fn: fn(&mut dyn Any, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>>,
}

trait FileVTable {
    const VTABLE: &'static RawFileVTable;
}

impl<F> FileVTable for F
where
    F: File,
{
    const VTABLE: &'static RawFileVTable = &RawFileVTable {
        poll_read_fn: |file, cx, buf| {
            let file = file.downcast_mut::<Self>().unwrap();
            file.poll_read(cx, buf)
        },
    };
}

pub trait FileSystem: Debug + Sync + Send + 'static {
    type File: File;

    /// Open a file at a given path.
    fn open(&self, path: &str) -> impl Future<Output = Result<Self::File>> + Send;

    /// Returns if this filesystem is able to handle the provided path.
    fn can_handle_path(&self, path: &str) -> bool;
}

/// A boxed future.
pub type FileSystemFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Debug)]
pub struct AnyFileSystem {
    pub(crate) vtable: &'static RawFileSystemVTable,
    pub(crate) filesystem: Arc<dyn Any + Sync + Send>,
}

impl AnyFileSystem {
    pub fn from_filesystem<F>(fs: F) -> Self
    where
        F: FileSystem,
    {
        AnyFileSystem {
            vtable: F::VTABLE,
            filesystem: Arc::new(fs),
        }
    }

    pub fn call_open<'a>(&'a self, path: &'a str) -> FileSystemFuture<'a, Result<AnyFile>> {
        (self.vtable.open_fn)(self.filesystem.as_ref(), path)
    }

    pub fn call_can_handle_path(&self, path: &str) -> bool {
        (self.vtable.can_handle_path_fn)(self.filesystem.as_ref(), path)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawFileSystemVTable {
    open_fn: for<'a> fn(fs: &'a dyn Any, path: &'a str) -> FileSystemFuture<'a, Result<AnyFile>>,
    can_handle_path_fn: fn(fs: &dyn Any, path: &str) -> bool,
}

trait FileSystemVTable {
    const VTABLE: &'static RawFileSystemVTable;
}

impl<S> FileSystemVTable for S
where
    S: FileSystem,
{
    const VTABLE: &'static RawFileSystemVTable = &RawFileSystemVTable {
        open_fn: |fs, path| {
            let fs = fs.downcast_ref::<Self>().unwrap();
            Box::pin(async {
                let file = fs.open(path).await?;
                Ok(AnyFile::from_file(file))
            })
        },

        can_handle_path_fn: |fs, path| {
            let fs = fs.downcast_ref::<Self>().unwrap();
            fs.can_handle_path(path)
        },
    };
}
