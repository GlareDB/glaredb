use std::io::SeekFrom;
use std::path::{Component, Path};
use std::sync::Arc;
use std::task::{Context, Poll};

use glaredb_error::{DbError, Result, not_implemented};

use super::dir_list::{DirEntry, DirList, NotImplementedDirList};
use super::{File, FileOpenContext, FileStat, FileSystem, FileType, OpenFlags};
use crate::buffer::buffer_manager::{AsRawBufferManager, RawBufferManager};
use crate::buffer::db_vec::DbVec;

#[derive(Debug)]
pub struct MemoryFileSystem {
    /// Manager to use for allocating file buffers.
    buffer_manager: RawBufferManager,
    /// Simple mapping of a flat name to byte buffer.
    files: scc::HashMap<String, Arc<DbVec<u8>>>,
}

impl MemoryFileSystem {
    pub fn new(buffer_manager: &impl AsRawBufferManager) -> Self {
        MemoryFileSystem {
            buffer_manager: buffer_manager.as_raw_buffer_manager(),
            files: scc::HashMap::new(),
        }
    }

    // TODO: Probably remove when proper fs write support is in.
    //
    // This is mostly useful for testing right now.
    pub fn insert(&self, path: impl Into<String>, file: impl AsRef<[u8]>) -> Result<()> {
        let bs = DbVec::new_from_slice(&self.buffer_manager, file)?;
        self.files.upsert(path.into(), Arc::new(bs));

        Ok(())
    }
}

impl FileSystem for MemoryFileSystem {
    type File = MemoryFileHandle;
    type State = ();
    type DirList = MemoryFileList;

    fn state_from_context(&self, _context: FileOpenContext) -> Result<Self::State> {
        Ok(())
    }

    async fn open(&self, flags: OpenFlags, path: &str, _state: &()) -> Result<Self::File> {
        if flags.is_write() {
            not_implemented!("write support for memory filesystem")
        }
        if flags.is_create() {
            not_implemented!("create support for memory filesystem")
        }

        let path = get_normalized_file_name(Path::new(path))?;
        let buffer = self
            .files
            .get(path)
            .map(|ent| ent.clone())
            .ok_or_else(|| DbError::new(format!("Cannot find file '{path}'")))?;

        Ok(MemoryFileHandle {
            path: path.to_string(),
            pos: 0,
            buffer,
        })
    }

    async fn stat(&self, path: &str, _state: &()) -> Result<Option<FileStat>> {
        let path = get_normalized_file_name(Path::new(path))?;
        if !self.files.contains(path) {
            return Ok(None);
        }

        Ok(Some(FileStat {
            file_type: FileType::File,
        }))
    }

    fn read_dir(&self, prefix: &str, _state: &Self::State) -> Self::DirList {
        let mut paths = Vec::new();
        self.files.scan(|path, _| {
            // TODO: ?
            if path.starts_with(prefix) {
                paths.push(DirEntry {
                    path: path.clone(),
                    file_type: FileType::File,
                });
            }
        });

        // TODO: Probably need to dedup, scc hash map can scan duplicates in
        // case of concurrent resizes.

        MemoryFileList { paths }
    }

    fn can_handle_path(&self, path: &str) -> bool {
        let path = Path::new(path);
        // TODO: Have separate function that doesn't return error.
        get_normalized_file_name(path).is_ok()
    }
}

#[derive(Debug)]
pub struct MemoryFileList {
    paths: Vec<DirEntry>,
}

impl DirList for MemoryFileList {
    fn poll_list(&mut self, _cx: &mut Context, paths: &mut Vec<DirEntry>) -> Poll<Result<usize>> {
        let n = self.paths.len();
        paths.append(&mut self.paths);
        Poll::Ready(Ok(n))
    }
}

#[derive(Debug)]
pub struct MemoryFileHandle {
    path: String,
    pos: usize,
    buffer: Arc<DbVec<u8>>,
}

impl MemoryFileHandle {
    /// Create an ephemeral file handle containg the provided bytes.
    ///
    /// The seek position will be at the beginning of the buffer.
    ///
    /// This file handle will not be associated with any filesystem (and is
    /// really only useful for tests).
    pub fn from_bytes(manager: &impl AsRawBufferManager, bytes: impl AsRef<[u8]>) -> Result<Self> {
        let bytes = bytes.as_ref();
        let buffer = DbVec::new_from_slice(manager, bytes)?;

        Ok(MemoryFileHandle {
            path: String::new(),
            pos: 0,
            buffer: Arc::new(buffer),
        })
    }
}

impl File for MemoryFileHandle {
    fn path(&self) -> &str {
        &self.path
    }

    fn size(&self) -> usize {
        self.buffer.len()
    }

    fn poll_read(&mut self, _cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        let rem = self.buffer.len() - self.pos;
        let count = usize::min(buf.len(), rem);

        let slice = self.buffer.as_slice();
        let src = &slice[self.pos..(self.pos + count)];
        let dest = &mut buf[..count];
        dest.copy_from_slice(src);

        self.pos += count;

        Poll::Ready(Ok(count))
    }

    fn poll_write(&mut self, _cx: &mut Context, _buf: &[u8]) -> Poll<Result<usize>> {
        Poll::Ready(Err(DbError::new(
            "Write unsupported for memory file handle",
        ))) // For now
    }

    fn poll_seek(&mut self, _cx: &mut Context, _seek: SeekFrom) -> Poll<Result<()>> {
        Poll::Ready(Err(DbError::new("Seek unsupported for memory file handle"))) // For now
    }

    fn poll_flush(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Err(DbError::new(
            "Flush unsupported for memory file handle",
        ))) // For now
    }
}

/// Gets a normalized file name that works with our in-memory file system
/// implementation.
///
/// Current rules for our filesystem:
///
/// - No directories permitted other than root or curr dir.
/// - Assume that current directory is the root directory.
fn get_normalized_file_name(path: &Path) -> Result<&str> {
    let mut components = path.components();
    match components.next() {
        Some(Component::RootDir) | Some(Component::CurDir) => (),
        Some(Component::Normal(s)) => {
            if components.next().is_some() {
                return Err(DbError::new(
                    "Directories not supported in memory fs provider",
                ));
            }

            return s
                .to_str()
                .ok_or_else(|| DbError::new("Unable to convert os string to string"));
        }
        Some(_) => return Err(DbError::new("Invalid component in path")),
        None => return Err(DbError::new("Path is empty")),
    }

    // We're either in '/' or './' (same thing)
    match components.next() {
        Some(Component::Normal(s)) => {
            if components.next().is_some() {
                return Err(DbError::new(
                    "Directories not supported in WASM fs provider",
                ));
            }

            Ok(s.to_str()
                .ok_or_else(|| DbError::new("Unable to convert os string to string"))?)
        }
        _ => Err(DbError::new("Invalid path component")),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::runtime::filesystem::dir_list::DirListExt;
    use crate::util::future::block_on;
    use crate::util::task::noop_context;

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

    #[test]
    fn memory_file_read_complete() {
        let mut handle = MemoryFileHandle::from_bytes(&DefaultBufferManager, b"hello").unwrap();
        let mut out = vec![0; 10];

        let poll = handle
            .poll_read(&mut noop_context(), &mut out)
            .map(|r| r.unwrap());
        assert_eq!(Poll::Ready(5), poll);
        assert_eq!(b"hello", &out[0..5]);

        let poll = handle
            .poll_read(&mut noop_context(), &mut out)
            .map(|r| r.unwrap());
        assert_eq!(Poll::Ready(0), poll);
    }

    #[test]
    fn memory_file_read_partial() {
        let mut handle = MemoryFileHandle::from_bytes(&DefaultBufferManager, b"hello").unwrap();
        let mut out = vec![0; 4];

        let poll = handle
            .poll_read(&mut noop_context(), &mut out)
            .map(|r| r.unwrap());
        assert_eq!(Poll::Ready(4), poll);
        assert_eq!(b"hell", &out[0..4]);

        let poll = handle
            .poll_read(&mut noop_context(), &mut out)
            .map(|r| r.unwrap());
        assert_eq!(Poll::Ready(1), poll);
        assert_eq!(b"o", &out[0..1]);

        let poll = handle
            .poll_read(&mut noop_context(), &mut out)
            .map(|r| r.unwrap());
        assert_eq!(Poll::Ready(0), poll);
    }

    // #[test]
    // fn memory_list_prefix() {
    //     let fs = MemoryFileSystem::new(&DefaultBufferManager);

    //     fs.insert("file_a.parquet", []).unwrap();
    //     fs.insert("file_b.parquet", []).unwrap();
    //     fs.insert("temp_a.parquet", []).unwrap();
    //     fs.insert("file_c.parquet", []).unwrap();
    //     fs.insert("file_d.parquet", []).unwrap();

    //     let mut paths = Vec::new();
    //     block_on(fs.read_dir("file_", &()).list_all(&mut paths)).unwrap();

    //     let expected = [
    //         "file_a.parquet".to_string(),
    //         "file_b.parquet".to_string(),
    //         "file_c.parquet".to_string(),
    //         "file_d.parquet".to_string(),
    //     ];
    //     assert_eq!(&expected, paths.as_slice());
    // }

    // #[test]
    // fn memory_list_glob() {
    //     let fs = MemoryFileSystem::new(&DefaultBufferManager);

    //     fs.insert("file_a.parquet", []).unwrap();
    //     fs.insert("file_b.parquet", []).unwrap();
    //     fs.insert("temp_a.parquet", []).unwrap();
    //     fs.insert("file_c.parquet", []).unwrap();
    //     fs.insert("file_d.parquet", []).unwrap();

    //     let mut paths = Vec::new();
    //     block_on(
    //         fs.glob_list("file_*.parquet", &())
    //             .unwrap()
    //             .expand_all(&mut paths),
    //     )
    //     .unwrap();

    //     let expected = [
    //         "file_a.parquet".to_string(),
    //         "file_b.parquet".to_string(),
    //         "file_c.parquet".to_string(),
    //         "file_d.parquet".to_string(),
    //     ];
    //     assert_eq!(&expected, paths.as_slice());

    //     let mut paths = Vec::new();
    //     block_on(
    //         fs.glob_list("*.parquet", &())
    //             .unwrap()
    //             .expand_all(&mut paths),
    //     )
    //     .unwrap();

    //     let expected = [
    //         "file_a.parquet".to_string(),
    //         "file_b.parquet".to_string(),
    //         "file_c.parquet".to_string(),
    //         "file_d.parquet".to_string(),
    //         "temp_a.parquet".to_string(),
    //     ];
    //     assert_eq!(&expected, paths.as_slice());
    // }
}
