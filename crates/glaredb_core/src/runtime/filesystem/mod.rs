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
    ///
    /// A return of Ok(0) indicates either an EOF or the provided buffer has a
    /// length of zero.
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

    pub fn call_size(&self) -> usize {
        // TODO: We could probably just stick the size on the struct.
        (self.vtable.size_fn)(self.file.as_ref())
    }

    pub fn call_poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        (self.vtable.poll_read_fn)(self.file.as_mut(), cx, buf)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawFileVTable {
    size_fn: fn(&dyn Any) -> usize,
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
        size_fn: |file| {
            let file = file.downcast_ref::<Self>().unwrap();
            file.size()
        },

        poll_read_fn: |file, cx, buf| {
            let file = file.downcast_mut::<Self>().unwrap();
            file.poll_read(cx, buf)
        },
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    File,
    Directory,
}

impl FileType {
    pub const fn is_file(&self) -> bool {
        matches!(self, FileType::File)
    }

    pub const fn is_dir(&self) -> bool {
        matches!(self, FileType::Directory)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStat {
    pub file_type: FileType,
}

pub trait FileSystem: Debug + Sync + Send + 'static {
    type File: File;

    /// Open a file at a given path.
    fn open(&self, path: &str) -> impl Future<Output = Result<Self::File>> + Sync + Send;

    /// Stat the file.
    ///
    /// This should return Ok(None) if the request was successful, but the file
    /// doesn't exist.
    fn stat(&self, path: &str) -> impl Future<Output = Result<Option<FileStat>>> + Sync + Send;

    /// Returns if this filesystem is able to handle the provided path.
    fn can_handle_path(&self, path: &str) -> bool;
}

/// A boxed future.
pub type FileSystemFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Sync + Send + 'a>>;

#[derive(Debug, Clone)]
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

    /// Like `call_open`, but returns a static boxed future.
    // TODO: I don't really want to do this, but I also don't care enough to
    // whip out unsafe since it's just for opening a file.
    pub fn call_open_static(
        &self,
        path: impl Into<String>,
    ) -> FileSystemFuture<'static, Result<AnyFile>> {
        (self.vtable.open_static_fn)(self.clone(), path.into())
    }

    pub fn call_stat<'a>(
        &'a self,
        path: &'a str,
    ) -> FileSystemFuture<'a, Result<Option<FileStat>>> {
        (self.vtable.stat_fn)(self.filesystem.as_ref(), path)
    }

    pub fn call_can_handle_path(&self, path: &str) -> bool {
        (self.vtable.can_handle_path_fn)(self.filesystem.as_ref(), path)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawFileSystemVTable {
    open_fn: for<'a> fn(fs: &'a dyn Any, path: &'a str) -> FileSystemFuture<'a, Result<AnyFile>>,
    open_static_fn:
        fn(fs: AnyFileSystem, path: String) -> FileSystemFuture<'static, Result<AnyFile>>,

    stat_fn: for<'a> fn(
        fs: &'a dyn Any,
        path: &'a str,
    ) -> FileSystemFuture<'a, Result<Option<FileStat>>>,
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

        open_static_fn: |any_fs, path| {
            Box::pin(async move {
                let fs = any_fs.filesystem.downcast_ref::<Self>().unwrap();
                let file = fs.open(&path).await?;
                Ok(AnyFile::from_file(file))
            })
        },

        stat_fn: |fs, path| {
            let fs = fs.downcast_ref::<Self>().unwrap();
            Box::pin(async { fs.stat(path).await })
        },

        can_handle_path_fn: |fs, path| {
            let fs = fs.downcast_ref::<Self>().unwrap();
            fs.can_handle_path(path)
        },
    };
}
