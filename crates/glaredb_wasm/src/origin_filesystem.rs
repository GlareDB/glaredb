use std::io;
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::directory::DirHandleNotImplemented;
use glaredb_core::runtime::filesystem::{
    FileHandle,
    FileOpenContext,
    FileStat,
    FileSystem,
    FileType,
    OpenFlags,
};
use glaredb_error::{DbError, OptionExt, Result, ResultExt};
use url::Url;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    DomException,
    FileSystemDirectoryHandle,
    FileSystemFileHandle,
    FileSystemGetFileOptions,
    FileSystemSyncAccessHandle,
    window,
};

use crate::errors::json_stringify;
use crate::http::FakeSyncSendFuture;

#[derive(Debug)]
pub struct OriginFileHandle {
    /// Path to the file.
    path: String,
    /// Size in bytes of the file.
    len: u64,
    /// Current position within the file.
    pos: u64,
    /// Web sys handle to the file.
    handle: FileSystemSyncAccessHandle,
}

// Sync makes sense, I'm not sure why websys doesn't make it sync automatically.
//
// Send is something we should probably think about a bit more, and maybe have
// the Send restriction optional for the pipeline/system runtime.
//
// But right now, this is fine. We can't actually send it anywhere.
unsafe impl Sync for OriginFileHandle {}
unsafe impl Send for OriginFileHandle {}

impl FileHandle for OriginFileHandle {
    fn path(&self) -> &str {
        &self.path
    }

    fn size(&self) -> u64 {
        self.len
    }

    fn poll_read(&mut self, _cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        // TODO: Loses error context.
        let n = self
            .handle
            .read_with_u8_array(buf)
            .map_err(|_val| DbError::new("Failed to read from file"))?;
        let n = n as usize;

        self.pos += n as u64;

        Poll::Ready(Ok(n))
    }

    fn poll_write(&mut self, _cx: &mut Context, _buf: &[u8]) -> Poll<Result<usize>> {
        // yet
        Poll::Ready(Err(DbError::new(
            "Write for origin file system not yet supported",
        )))
    }

    fn poll_seek(&mut self, _cx: &mut Context, seek: io::SeekFrom) -> Poll<Result<()>> {
        match seek {
            io::SeekFrom::Start(count) => self.pos = count,
            io::SeekFrom::End(count) => {
                if count > 0 {
                    // It's legal to seek beyond the end, but the read may fail.
                    self.pos = self.len + count as u64;
                } else {
                    let count = count.unsigned_abs();
                    if count > self.len {
                        return Poll::Ready(Err(DbError::new(
                            "Cannot seek to before beginning of file",
                        )));
                    }
                    self.pos = self.len - count;
                }
            }
            io::SeekFrom::Current(count) => {
                if count > 0 {
                    // Just add to current position, as above, it's legal to seek beyond the end.
                    self.pos += count as u64;
                } else {
                    let count = count.unsigned_abs();
                    if count > self.pos {
                        return Poll::Ready(Err(DbError::new(
                            "Cannot seek to before beginning of file",
                        )));
                    }
                    self.pos -= count;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        // TOOD: Loses error context.
        self.handle
            .flush()
            .map_err(|_| DbError::new("Failed to flush file"))?;
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub struct OriginFileSystem {}

impl OriginFileSystem {
    /// Get a sync access handle to a file at the given path.
    ///
    /// Public for tests. Once we have write support, that should be used
    /// instead for dogfooding.
    pub async fn open_sync_access_handle(
        &self,
        path: &str,
        create: bool,
    ) -> Result<Option<FileSystemSyncAccessHandle>> {
        // javascript bullshit
        let fut = Box::pin(async move {
            let window = window().required("Global window object")?;

            let root_object = JsFuture::from(window.navigator().storage().get_directory())
                .await
                .map_err(|_| DbError::new("Failed to get root directory"))?;
            let root: FileSystemDirectoryHandle = root_object
                .dyn_into()
                .map_err(|_| DbError::new("Object not a FileSystemDirectoryHandle"))?;

            let options = FileSystemGetFileOptions::new();
            options.set_create(create);

            // TODO: I don't know what happens if 'path' is a directory, or
            // contains a directory to a file. Do I need to manually walk
            // directories?
            let handle =
                match JsFuture::from(root.get_file_handle_with_options(path, &options)).await {
                    Ok(handle) => match handle.dyn_into::<FileSystemFileHandle>() {
                        Ok(handle) => handle,
                        Err(_) => return Err(DbError::new("Object not a FileSystemFileHandle")),
                    },
                    Err(err) => {
                        // Try to turn it into a DOMException
                        match err.dyn_into::<DomException>() {
                            Ok(exception) if exception.code() == DomException::NOT_FOUND_ERR => {
                                return Ok(None);
                            }
                            Ok(exception) => {
                                return Err(DbError::new("Failed to get file handle")
                                    .with_field("js_name", exception.name())
                                    .with_field("message", exception.message()));
                            }
                            Err(err) => {
                                return Err(DbError::new(
                                    "Failed to get file handle, not a dom exception",
                                )
                                .with_field("stringified_exception", json_stringify(&err)));
                            }
                        }
                    }
                };

            // _Really_ get the handle.
            // TODO: This needs to happen in a dedicated web worker.
            // TODO: Options for read/read write
            let handle = JsFuture::from(handle.create_sync_access_handle())
                .await
                .map_err(|_| DbError::new("Failed to create sync access handle"))?;

            Ok(Some(FileSystemSyncAccessHandle::from(handle)))
        });

        unsafe { FakeSyncSendFuture::new(fut).await }
    }
}

impl FileSystem for OriginFileSystem {
    const NAME: &str = "Origin Private";

    type FileHandle = OriginFileHandle;
    type ReadDirHandle = DirHandleNotImplemented;
    type State = ();

    async fn load_state(&self, _context: FileOpenContext<'_>) -> Result<Self::State> {
        Ok(())
    }

    async fn open(
        &self,
        flags: OpenFlags,
        path: &str,
        _state: &Self::State,
    ) -> Result<Self::FileHandle> {
        if flags.is_write() {
            // yet
            return Err(DbError::new("Write not supported"));
        }
        if flags.is_create() {
            // yet
            return Err(DbError::new("Create not supported"));
        }

        let url = Url::parse(path).context("Failed to parse path as url")?;
        if url.scheme() != "browser" {
            return Err(DbError::new(format!(
                "Expected 'browser' as scheme, got '{}'",
                url.scheme()
            )));
        }

        let path = url.path();
        let handle = self
            .open_sync_access_handle(path, false)
            .await?
            .ok_or_else(|| DbError::new("Missing file for path '{path}'"))?;

        let len = handle
            .get_size()
            .map_err(|_| DbError::new("Failed to get file size"))?;

        Ok(OriginFileHandle {
            path: path.to_string(),
            len: len as u64,
            pos: 0,
            handle,
        })
    }

    async fn stat(&self, path: &str, _state: &Self::State) -> Result<Option<FileStat>> {
        let url = Url::parse(path).context("Failed to parse path as url")?;
        if url.scheme() != "browser" {
            return Err(DbError::new(format!(
                "Expected 'browser' as scheme, got '{}'",
                url.scheme()
            )));
        }

        let path = url.path();

        match self.open_sync_access_handle(path, false).await? {
            Some(_) => Ok(Some(FileStat {
                file_type: FileType::File,
            })),
            None => Ok(None),
        }
    }

    fn can_handle_path(&self, path: &str) -> bool {
        // TODO: Requring a valid url for the path is ugly, but I'm doing it for
        // now just to test things out.
        matches!(Url::parse(path), Ok(url) if url.scheme() == "browser")
    }
}
