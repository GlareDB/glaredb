use std::fmt::Debug;
use std::task::{Context, Poll};

use glaredb_error::{DbError, Result};

use super::FileType;

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub path: String,
    pub file_type: FileType,
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
