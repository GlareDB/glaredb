use std::io::SeekFrom;
use std::path::{Component, Path};
use std::sync::Arc;
use std::task::{Context, Poll};

use glaredb_error::{DbError, Result, not_implemented};

use super::{File, FileStat, FileSystem, FileType, OpenFlags};
use crate::buffer::buffer_manager::{AsRawBufferManager, RawBufferManager};
use crate::buffer::typed::ByteBuffer;

#[derive(Debug)]
pub struct MemoryFileSystem {
    /// Manager to use for allocating file buffers.
    #[allow(unused)] // Will be used for creates
    buffer_manager: RawBufferManager,
    /// Simple mapping of a flat name to byte buffer.
    files: scc::HashMap<String, Arc<ByteBuffer>>,
}

impl MemoryFileSystem {
    pub fn new(buffer_manager: &impl AsRawBufferManager) -> Self {
        MemoryFileSystem {
            buffer_manager: buffer_manager.as_raw_buffer_manager(),
            files: scc::HashMap::new(),
        }
    }
}

impl FileSystem for MemoryFileSystem {
    type File = MemoryFileHandle;

    async fn open(&self, flags: OpenFlags, path: &str) -> Result<Self::File> {
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

        Ok(MemoryFileHandle { pos: 0, buffer })
    }

    async fn stat(&self, path: &str) -> Result<Option<FileStat>> {
        let path = get_normalized_file_name(Path::new(path))?;
        if !self.files.contains(path) {
            return Ok(None);
        }

        Ok(Some(FileStat {
            file_type: FileType::File,
        }))
    }

    fn can_handle_path(&self, path: &str) -> bool {
        let path = Path::new(path);
        // TODO: Have separate function that doesn't return error.
        get_normalized_file_name(path).is_ok()
    }
}

#[derive(Debug)]
pub struct MemoryFileHandle {
    pos: usize,
    buffer: Arc<ByteBuffer>,
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
        let mut buffer = ByteBuffer::try_with_capacity(manager, bytes.len())?;

        let slice = &mut buffer.as_slice_mut()[..bytes.len()]; // We may have allocated more than requested
        slice.copy_from_slice(bytes);

        Ok(MemoryFileHandle {
            pos: 0,
            buffer: Arc::new(buffer),
        })
    }
}

impl File for MemoryFileHandle {
    fn size(&self) -> usize {
        self.buffer.capacity()
    }

    fn poll_read(&mut self, _cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        let rem = self.buffer.capacity() - self.pos;
        let count = usize::min(buf.len(), rem);

        let src = &self.buffer.as_slice()[self.pos..(self.pos + count)];
        let dest = &mut buf[..count];
        dest.copy_from_slice(src);

        self.pos += count;

        Poll::Ready(Ok(count))
    }

    fn poll_write(&mut self, _buf: &[u8]) -> Poll<Result<usize>> {
        Poll::Ready(Err(DbError::new(
            "Write unsupported for memory file handle",
        ))) // For now
    }

    fn poll_seek(&mut self, _seek: SeekFrom) -> Poll<Result<()>> {
        Poll::Ready(Err(DbError::new("Seek unsupported for memory file handle"))) // For now
    }

    fn poll_flush(&mut self) -> Poll<Result<()>> {
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
    use crate::buffer::buffer_manager::NopBufferManager;
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
        let mut handle = MemoryFileHandle::from_bytes(&NopBufferManager, b"hello").unwrap();
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
        let mut handle = MemoryFileHandle::from_bytes(&NopBufferManager, b"hello").unwrap();
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
}
