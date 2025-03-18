use std::collections::HashMap;
use std::path::{Component, Path};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use glaredb_error::{DbError, Result};
use parking_lot::Mutex;

use crate::exp::AsyncReadStream;
use crate::{FileSink, FileSource};

/// Memory-backed filesystem provider.
///
/// This provides a flat structure where every "file" exists at the root of the
/// filesystem.
///
/// The primary use case for this is wasm, but may be used to provide a
/// "memory-only" local instance too. The wasm use case might change into
/// hooking into the browser's localstorage or filesystem api.
#[derive(Debug, Default)]
pub struct MemoryFileSystem {
    /// A simple file name -> file bytes mapping.
    files: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl MemoryFileSystem {
    pub fn register_file(&self, path: &Path, content: Bytes) -> Result<()> {
        let name = get_normalized_file_name(path)?;
        self.files.lock().insert(name.to_string(), content);
        Ok(())
    }

    pub fn list_files(&self) -> Vec<String> {
        self.files.lock().keys().cloned().collect()
    }
}

impl MemoryFileSystem {
    pub fn file_source(&self, path: &Path) -> Result<Box<dyn FileSource>> {
        let name = get_normalized_file_name(path)?;
        let content = self
            .files
            .lock()
            .get(name)
            .cloned()
            .ok_or_else(|| DbError::new(format!("Missing file for '{name}'")))?;

        Ok(Box::new(MemoryFile { content }))
    }

    pub fn file_sink(&self, path: &Path) -> Result<Box<dyn FileSink>> {
        let name = get_normalized_file_name(path)?;
        Ok(Box::new(MemoryFileSink {
            name: name.to_string(),
            buf: Vec::new(),
            files: self.files.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct MemoryFile {
    content: Bytes,
}

impl FileSource for MemoryFile {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        let result = if start + len > self.content.len() {
            Err(DbError::new("Byte range out of bounds"))
        } else {
            let bs = self.content.slice(start..start + len);
            Ok(bs)
        };
        async move { result }.boxed()
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        MemoryFileStream {
            content: self.content.clone(),
            curr: 0,
        }
        .boxed()
    }

    fn size(&mut self) -> BoxFuture<Result<usize>> {
        let size = self.content.len();
        async move { Ok(size) }.boxed()
    }
}

#[derive(Debug)]
pub struct MemoryFileSink {
    name: String,
    buf: Vec<u8>,
    files: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl FileSink for MemoryFileSink {
    fn write_all(&mut self, buf: Bytes) -> BoxFuture<'static, Result<()>> {
        self.buf.extend_from_slice(buf.as_ref());
        async { Ok(()) }.boxed()
    }

    fn finish(&mut self) -> BoxFuture<'static, Result<()>> {
        let bytes = Bytes::from(std::mem::take(&mut self.buf));
        self.files.lock().insert(self.name.clone(), bytes);
        async { Ok(()) }.boxed()
    }
}

#[derive(Debug)]
struct MemoryFileStream {
    content: Bytes,
    curr: usize,
}

impl MemoryFileStream {
    fn read_next(&mut self) -> Result<Bytes> {
        const STREAM_BUF_SIZE: usize = 4 * 1024;

        let buf = if self.content.len() - self.curr < STREAM_BUF_SIZE {
            self.content.clone()
        } else {
            self.content.slice(self.curr..(self.curr + STREAM_BUF_SIZE))
        };

        self.curr += buf.len();

        Ok(buf)
    }
}

impl Stream for MemoryFileStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.curr >= self.content.len() {
            return Poll::Ready(None);
        }

        let result = self.read_next();
        Poll::Ready(Some(result))
    }
}

/// Stream read from a shared byte slice.
///
/// Also useful for tests.
#[derive(Debug)]
pub struct MemoryRead {
    bytes: Bytes,
    offset: usize,
}

impl MemoryRead {
    pub fn new(bytes: impl Into<Bytes>) -> Self {
        MemoryRead {
            bytes: bytes.into(),
            offset: 0,
        }
    }
}

impl AsyncReadStream for MemoryRead {
    fn poll_read(&mut self, _cx: &mut Context, buf: &mut [u8]) -> Result<Poll<Option<usize>>> {
        if self.offset >= self.bytes.len() {
            return Ok(Poll::Ready(None));
        }

        let rem = self.bytes.len() - self.offset;
        let count = usize::min(buf.len(), rem);

        let src = &self.bytes[self.offset..(self.offset + count)];
        let dest = &mut buf[0..count];

        dest.copy_from_slice(src);
        self.offset += count;

        Ok(Poll::Ready(Some(count)))
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
                return Err(DbError::new(
                    "Directories not supported in WASM fs provider",
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

// #[cfg(test)]
// mod tests {
//     use std::path::PathBuf;

//     use crate::util::task::noop_context;

//     use super::*;

//     #[test]
//     fn valid_file_names() {
//         assert_eq!(
//             "test.parquet",
//             get_normalized_file_name(&PathBuf::from("test.parquet")).unwrap()
//         );
//         assert_eq!(
//             "test.parquet",
//             get_normalized_file_name(&PathBuf::from("/test.parquet")).unwrap()
//         );
//         assert_eq!(
//             "test.parquet",
//             get_normalized_file_name(&PathBuf::from("./test.parquet")).unwrap()
//         );
//     }

//     #[test]
//     fn invalid_file_names() {
//         get_normalized_file_name(&PathBuf::from("../test.parquet")).unwrap_err();
//         get_normalized_file_name(&PathBuf::from("dir/test.parquet")).unwrap_err();
//         get_normalized_file_name(&PathBuf::from("./dir/test.parquet")).unwrap_err();
//         get_normalized_file_name(&PathBuf::from("/dir/test.parquet")).unwrap_err();
//         get_normalized_file_name(&PathBuf::from("/../test.parquet")).unwrap_err();
//     }

//     #[test]
//     fn memory_read_complete() {
//         let mut read = Box::pin(MemoryRead::new(b"hello".to_vec()));
//         let mut out = vec![0; 8];

//         let poll = read
//             .as_mut()
//             .poll_read(&mut noop_context(), &mut out)
//             .unwrap();
//         assert_eq!(Poll::Ready(Some(5)), poll);

//         assert_eq!(b"hello\0\0\0".to_vec(), out);

//         let poll = read
//             .as_mut()
//             .poll_read(&mut noop_context(), &mut out)
//             .unwrap();
//         assert_eq!(Poll::Ready(None), poll);
//     }

//     #[test]
//     fn memory_read_partial() {
//         let mut read = Box::pin(MemoryRead::new(b"hello".to_vec()));
//         let mut out = vec![0; 4];

//         let poll = read
//             .as_mut()
//             .poll_read(&mut noop_context(), &mut out)
//             .unwrap();
//         assert_eq!(Poll::Ready(Some(4)), poll);

//         assert_eq!(b"hell".to_vec(), out);

//         out.fill(0);

//         let poll = read
//             .as_mut()
//             .poll_read(&mut noop_context(), &mut out)
//             .unwrap();
//         assert_eq!(Poll::Ready(Some(1)), poll);

//         assert_eq!(b"o\0\0\0".to_vec(), out);
//     }
// }
