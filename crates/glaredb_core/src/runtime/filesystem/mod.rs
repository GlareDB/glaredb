pub mod dispatch;

use std::any::Any;
use std::fmt::Debug;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use glaredb_error::Result;

/// A boxed future.
pub type FileFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub trait File: Debug + Sync + Send + 'static {
    /// Get the size in bytes of this file.
    fn size(&self) -> usize;

    /// Read from the current position into the buffer, returning the number of
    /// bytes read.
    fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize>> + Send;

    /// Write at the current position, returning the number of bytes written.
    fn write(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize>> + Send;

    /// Seek the provided position in the file.
    fn seek(&mut self, seek: io::SeekFrom) -> impl Future<Output = Result<()>> + Send;

    /// Flush the file to disk (or persistent storage).
    fn flush(&mut self) -> impl Future<Output = Result<()>> + Send;
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

    pub fn call_read<'a>(&'a mut self, buf: &'a mut [u8]) -> FileFuture<'a, Result<usize>> {
        (self.vtable.read_fn)(self.file.as_mut(), buf)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawFileVTable {
    read_fn: for<'a> fn(&'a mut dyn Any, buf: &'a mut [u8]) -> FileFuture<'a, Result<usize>>,
}

trait FileVTable {
    const VTABLE: &'static RawFileVTable;
}

impl<F> FileVTable for F
where
    F: File,
{
    const VTABLE: &'static RawFileVTable = &RawFileVTable {
        read_fn: |file, buf| {
            let file = file.downcast_mut::<Self>().unwrap();
            Box::pin(async { file.read(buf).await })
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

    pub fn call_open<'a>(&'a self, path: &'a str) -> FileFuture<'a, Result<AnyFile>> {
        (self.vtable.open_fn)(self.filesystem.as_ref(), path)
    }

    pub fn call_can_handle_path(&self, path: &str) -> bool {
        (self.vtable.can_handle_path_fn)(self.filesystem.as_ref(), path)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawFileSystemVTable {
    open_fn: for<'a> fn(fs: &'a dyn Any, path: &'a str) -> FileFuture<'a, Result<AnyFile>>,
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
