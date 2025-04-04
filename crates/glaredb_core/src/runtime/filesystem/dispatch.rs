use glaredb_error::Result;

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
    pub fn register_filesystem<F>(&mut self, fs: F) -> Result<()>
    where
        F: FileSystem,
    {
        self.filesystems.push(AnyFileSystem::from_filesystem(fs));
        Ok(())
    }

    /// Try to find a filesystem that can handle `path`.
    pub fn filesystem_for_path(&self, path: &str) -> Option<&AnyFileSystem> {
        for fs in &self.filesystems {
            if fs.call_can_handle_path(path) {
                return Some(fs);
            }
        }
        None
    }
}
