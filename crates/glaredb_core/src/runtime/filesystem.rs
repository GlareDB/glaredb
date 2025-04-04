use std::any::Any;
use std::fmt::Debug;
use std::io;
use std::sync::Arc;

use glaredb_error::Result;

pub trait File: Debug + Sync + Send {
    /// Get the size in bytes of this file.
    fn size(&self) -> usize;

    /// Read from the current position into the buffer, returning the number of
    /// bytes read.
    fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize>>;

    /// Write at the current position, returning the number of bytes written.
    fn write(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize>>;

    /// Seek the provided position in the file.
    fn seek(&mut self, seek: io::SeekFrom) -> impl Future<Output = Result<()>>;

    /// Flush the file to disk (or persistent storage).
    fn flush(&mut self) -> impl Future<Output = Result<()>>;
}

#[derive(Debug)]
pub struct AnyFile {
    pub(crate) vtable: &'static RawFileVTable,
    pub(crate) file: Box<dyn Any + Sync + Send>,
}

impl AnyFile {}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawFileVTable {}

trait FileVTable {
    const VTABLE: &'static RawFileVTable;
}

impl<F> FileVTable for F
where
    F: File,
{
    const VTABLE: &'static RawFileVTable = &RawFileVTable {};
}

pub trait FileSystem: Debug + Sync + Send {
    type File: File;

    /// Open a file at a given path.
    fn open(&self, path: &str) -> impl Future<Output = Result<Self::File>>;

    /// Returns if this filesystem is able to handle the provided path.
    fn can_handle_path(&self, path: &str) -> bool;
}

#[derive(Debug)]
pub struct AnyFileSystem {
    pub(crate) vtable: &'static RawFileSystemVTable,
    pub(crate) filesystem: Arc<dyn Any + Sync + Send>,
}

impl AnyFileSystem {}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawFileSystemVTable {}

trait FileSystemVTable {
    const VTABLE: &'static RawFileSystemVTable = &RawFileSystemVTable {};
}

impl<S> FileSystemVTable for S
where
    S: FileSystem,
{
    const VTABLE: &'static RawFileSystemVTable = &RawFileSystemVTable {};
}
