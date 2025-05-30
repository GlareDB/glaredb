use glaredb_error::{DbError, Result};

use super::{AnyFileSystem, FileSystem};

#[derive(Debug)]
pub struct FileSystemDispatch {
    pub(crate) filesystems: Vec<AnyFileSystem>,
}

impl FileSystemDispatch {
    pub const fn empty() -> Self {
        FileSystemDispatch {
            filesystems: Vec::new(),
        }
    }

    /// Register a new filesystem with the dispatcher.
    pub fn register_filesystem<F>(&mut self, fs: F)
    where
        F: FileSystem,
    {
        self.filesystems.push(AnyFileSystem::from_filesystem(fs));
    }

    /// Try to find a filesystem that can handle `path`.
    ///
    /// File systems are checked in the order they were registered in. More
    /// generic filesystems should be registered last.
    pub fn filesystem_for_path_opt(&self, path: &str) -> Option<&AnyFileSystem> {
        self.filesystems
            .iter()
            .find(|&fs| fs.call_can_handle_path(path))
    }

    pub fn filesystem_for_path(&self, path: &str) -> Result<&AnyFileSystem> {
        self.filesystem_for_path_opt(path)
            .ok_or_else(|| DbError::new(format!("Could not find a filesystem to handle '{path}'")))
    }
}
