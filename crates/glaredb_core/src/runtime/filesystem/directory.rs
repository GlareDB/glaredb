use std::fmt::Debug;
use std::task::{Context, Poll};

use glaredb_error::{DbError, Result};

use super::FileType;

// TODO: Possibly make trait with 'path', 'file_type' methods.
//
// Possibly would make some of the path parsing/formatting clear for s3/gcs.
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// Path to the file or directory.
    ///
    /// Should not contain a trailing slash. Our globber makes assumptions about
    /// being able to split on '/'.
    pub(crate) path: String,
    /// File type of the entry.
    pub(crate) file_type: FileType,
}

impl DirEntry {
    /// Create a new dir entry with the given path and file type.
    pub fn new(mut path: String, file_type: FileType) -> Self {
        if path.ends_with('/') {
            path.pop();
        }

        DirEntry { path, file_type }
    }

    pub fn new_file(path: String) -> Self {
        Self::new(path, FileType::File)
    }

    pub fn new_dir(path: String) -> Self {
        Self::new(path, FileType::Directory)
    }
}

/// Handle for "reading a directory". Polling will internally move some cursor
/// forward until exhaustion.
pub trait ReadDirHandle: Debug + Sync + Send + Sized + 'static {
    /// List entries in the current directory.
    fn poll_list(&mut self, cx: &mut Context, ents: &mut Vec<DirEntry>) -> Poll<Result<usize>>;

    /// Descends into another directory relative to this one.
    ///
    /// This will return a new handle and will not modify this handle. The new
    /// handle should start from a fresh "listing" state.
    fn change_dir(&mut self, relative: impl Into<String>) -> Result<Self>;
}

#[derive(Debug, Clone)]
pub struct DirHandleNotImplemented;

impl ReadDirHandle for DirHandleNotImplemented {
    fn poll_list(&mut self, _cx: &mut Context, _ents: &mut Vec<DirEntry>) -> Poll<Result<usize>> {
        Poll::Ready(Err(DbError::new(
            "Dir handle not implemented for this file system",
        )))
    }

    fn change_dir(&mut self, _relative: impl Into<String>) -> Result<Self> {
        Err(DbError::new(
            "Dir handle not implemented for this file system",
        ))
    }
}
