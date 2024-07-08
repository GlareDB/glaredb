use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use rayexec_io::{
    filesystem::{FileReader, FileSystemProvider},
    AsyncReader,
};
use std::{
    collections::HashMap,
    path::{Component, Path},
};

/// Memory-backed filesystem provider for wasm.
///
/// This provides a flat structure where every "file" exists at the root of the
/// filesystem.
///
/// Eventually may be changed to hook into the browser filesystem api to provide
/// persistence across refreshes, and access to the filesystem across web
/// workers.
#[derive(Debug, Default)]
pub struct WasmMemoryFileSystem {
    /// A simple file name -> file bytes mapping.
    files: Mutex<HashMap<String, Bytes>>,
}

impl WasmMemoryFileSystem {
    pub fn register_file(&self, path: &Path, content: Bytes) -> Result<()> {
        let name = get_normalized_file_name(path)?;
        self.files.lock().insert(name.to_string(), content);
        Ok(())
    }

    pub fn list_files(&self) -> Vec<String> {
        self.files.lock().keys().cloned().collect()
    }
}

impl FileSystemProvider for WasmMemoryFileSystem {
    fn reader(&self, path: &Path) -> Result<Box<dyn FileReader>> {
        let name = get_normalized_file_name(path)?;
        let content = self
            .files
            .lock()
            .get(name)
            .cloned()
            .ok_or_else(|| RayexecError::new(format!("Missing file for '{name}'")))?;

        Ok(Box::new(WasmMemoryFile { content }))
    }
}

#[derive(Debug)]
pub struct WasmMemoryFile {
    content: Bytes,
}

impl AsyncReader for WasmMemoryFile {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        let result = if start + len > self.content.len() {
            Err(RayexecError::new("Byte range out of bounds"))
        } else {
            let bs = self.content.slice(start..start + len);
            Ok(bs)
        };
        async move { result }.boxed()
    }
}

impl FileReader for WasmMemoryFile {
    fn size(&mut self) -> BoxFuture<Result<usize>> {
        let size = self.content.len();
        async move { Ok(size) }.boxed()
    }
}

/// Gets a normalized file name that works with our in-memory file system
/// implementation.
///
/// Current rules for our filesystem:
///
/// - No directories permitted other than root or curr dir.
/// - Assume that current directory is the root directory.
pub fn get_normalized_file_name(path: &Path) -> Result<&str> {
    let mut components = path.components();
    match components.next() {
        Some(Component::RootDir) | Some(Component::CurDir) => (),
        Some(Component::Normal(s)) => {
            if components.next().is_some() {
                return Err(RayexecError::new(
                    "Directories not supported in WASM fs provider",
                ));
            }

            return s
                .to_str()
                .ok_or_else(|| RayexecError::new("Unable to convert os string to string"));
        }
        Some(_) => return Err(RayexecError::new("Invalid component in path")),
        None => return Err(RayexecError::new("Path is empty")),
    }

    // We're either in '/' or './' (same thing)
    match components.next() {
        Some(Component::Normal(s)) => {
            if components.next().is_some() {
                return Err(RayexecError::new(
                    "Directories not supported in WASM fs provider",
                ));
            }

            Ok(s.to_str()
                .ok_or_else(|| RayexecError::new("Unable to convert os string to string"))?)
        }
        _ => Err(RayexecError::new("Invalid path component")),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn valid_file_names() {
        assert_eq!(
            "test.parquet",
            get_normalized_file_name(&PathBuf::from("test.parquet")).unwrap()
        );
        assert_eq!(
            "test.parquet",
            get_normalized_file_name(&PathBuf::from("/test.parquet")).unwrap()
        );
        assert_eq!(
            "test.parquet",
            get_normalized_file_name(&PathBuf::from("./test.parquet")).unwrap()
        );
    }

    #[test]
    fn invalid_file_names() {
        get_normalized_file_name(&PathBuf::from("../test.parquet")).unwrap_err();
        get_normalized_file_name(&PathBuf::from("dir/test.parquet")).unwrap_err();
        get_normalized_file_name(&PathBuf::from("./dir/test.parquet")).unwrap_err();
        get_normalized_file_name(&PathBuf::from("/dir/test.parquet")).unwrap_err();
        get_normalized_file_name(&PathBuf::from("/../test.parquet")).unwrap_err();
    }
}
