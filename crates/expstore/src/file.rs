use crate::errors::{internal, ExpstoreError, Result};
use bytes::Buf;
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing::{trace, warn};

/// Default size in bytes for the buffer used uploading and downloading objects.
const OBJECT_BUFFER_DEFAULT_SIZE: usize = 32 * 1024 * 1024;

/// Local file cache for files stored in object storage.
#[derive(Debug)]
pub struct LocalCache {
    store: Box<dyn ObjectStore>,
    /// Path to where local files will be kept.
    local_cache_path: PathBuf,
    /// Number of bytes the local cache can use.
    local_cache_size: AtomicUsize,
    /// Number of bytes currently in use.
    local_cache_used: AtomicUsize,
}

impl LocalCache {
    pub fn new<O: ObjectStore>(
        store: O,
        local_cache_path: PathBuf,
        local_cache_size: usize,
    ) -> Self {
        LocalCache {
            store: Box::new(store),
            local_cache_path,
            local_cache_size: local_cache_size.into(),
            local_cache_used: 0.into(),
        }
    }

    fn open_local_file<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        let path = self.local_cache_path.join(path);
        trace!(?path, "opening local file");
        if let Some(parent) = path.parent() {
            trace!(?parent, "creating parent directory for local file");
            std::fs::create_dir_all(parent)?;
        }

        // TODO: Possibly direct io and/or io uring.
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        Ok(file)
    }

    fn remove_local_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = self.local_cache_path.join(path);
        fs::remove_file(&path)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct CloseError {
    pub file: MirroredFile,
    pub error: ExpstoreError,
}

/// A file that is mirrored with a file within some remote object store.
///
/// The directory struct on the local disk mirrors the directory structure with
/// the object store.
// TODO: We'll need to update the sync stats within the relevant methods.
pub struct MirroredFile {
    obj_relative: ObjectPath,
    local_relative: PathBuf,
    local: File,
    cache: Arc<LocalCache>,
    /// A lazily allocated buffer used for uploading and downloading objects.
    buf: Option<Vec<u8>>,
}

impl MirroredFile {
    /// Create a new file.
    ///
    /// This will not check if the file exists in the object store.
    pub async fn create(path: PathBuf, sync: Arc<LocalCache>) -> Result<MirroredFile> {
        let file = sync.open_local_file(&path)?;
        let obj_relative = to_object_path(&path)?;

        Ok(MirroredFile {
            obj_relative,
            local_relative: path,
            local: file,
            cache: sync,
            buf: None,
        })
    }

    /// Open an existing file from the object store.
    ///
    /// Errors if no file exists in the object store.
    pub async fn open_from_remote(path: PathBuf, sync: Arc<LocalCache>) -> Result<MirroredFile> {
        let file = sync.open_local_file(&path)?;
        let mut writer = io::BufWriter::with_capacity(OBJECT_BUFFER_DEFAULT_SIZE, file);

        let obj_relative = to_object_path(&path)?;
        let mut stream = sync.store.get(&obj_relative).await?.into_stream();

        while let Some(result) = stream.next().await {
            let mut reader = result?.reader();
            io::copy(&mut reader, &mut writer)?;
        }

        writer.flush()?;
        let (file, buf) = match writer.into_parts() {
            (file, Ok(buf)) => (file, buf),
            _ => return Err(internal!("writer panicked")),
        };

        Ok(MirroredFile {
            obj_relative,
            local_relative: path,
            local: file,
            cache: sync,
            buf: Some(buf),
        })
    }

    /// Sync self to the remote object, and remove this file from the file
    /// system.
    pub async fn sync_and_remove(mut self) -> Result<(), CloseError> {
        let mut buf = self
            .buf
            .take()
            .unwrap_or_else(|| vec![0; OBJECT_BUFFER_DEFAULT_SIZE]);

        // Sync then remove the file. Technically we should manually sync the
        // parent directory as well to ensure removal actually takes place.
        self.sync_remote(&mut buf)
            .await
            .and_then(|_| self.cache.remove_local_file(&self.local_relative))
            .map_err(|e| CloseError {
                file: self,
                error: e,
            })?;

        Ok(())
    }

    /// Sync this file to the remote object store, using the provided buffer for
    /// reading and writing the file.
    ///
    /// The file must have already been flushed to disk.
    ///
    /// Note that concurrent access during this method call is likely not
    /// correct.
    async fn sync_remote(&self, buf: &mut [u8]) -> Result<()> {
        let (id, mut writer) = self.cache.store.put_multipart(&self.obj_relative).await?;
        trace!(%id, "syncing file remote");

        // Note we're not using a buffered writer here. Each object store client
        // will internally buffer writes.
        let mut offset = 0;
        let file_len = self.local.metadata()?.len() as usize;
        while offset < file_len {
            let num_read = self.read_at(buf, offset as u64)?;
            if let Err(e) = writer.write_all(&buf[0..num_read]).await {
                if let Err(e) = self
                    .cache
                    .store
                    .abort_multipart(&self.obj_relative, &id)
                    .await
                {
                    warn!(?e, %id, "failed to abort multipart upload after failed write");
                    // Note that this is a best effort cleanup. We should not
                    // return the error resulting from the failed abort.
                }
                return Err(e.into());
            }
            offset += num_read;
        }

        if let Err(e) = writer.shutdown().await {
            if let Err(e) = self
                .cache
                .store
                .abort_multipart(&self.obj_relative, &id)
                .await
            {
                warn!(?e, %id, "failed to abort multipart upload after failed poll shutdown");
                // Similar to above, want to return the first error.
            }
            return Err(e.into());
        }

        Ok(())
    }
}

impl FileExt for MirroredFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        if cfg!(unix) {
            self.local.read_at(buf, offset)
        } else {
            todo!()
        }
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        if cfg!(unix) {
            self.local.write_at(buf, offset)
        } else {
            todo!()
        }
    }
}

impl Read for MirroredFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.local.read(buf)
    }
}

impl Seek for MirroredFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.local.seek(pos)
    }
}

// TODO: Have the mirrored file buffer. This will also allow us to track bytes
// written to the local cache.
impl Write for MirroredFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.local.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.local.flush()
    }
}

impl fmt::Debug for MirroredFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MirroredFile")
            .field("path", &self.obj_relative)
            .finish()
    }
}

/// Convert a file system path to an object store path.
///
/// The provided path must be relative and canonical.
///
/// Note that this doesn't use `ObjectPath::from_filesystem_path` since that
/// function will attempt to resolve the path using the local filesystem.
fn to_object_path<P: AsRef<Path>>(path: P) -> Result<ObjectPath> {
    let path = path.as_ref();
    let s = path
        .to_str()
        .ok_or_else(|| internal!("provided path not valid utf8: {:?}", path))?;
    if path.is_absolute() {
        return Err(internal!("path must be relative: {:?}", path));
    }
    let obj_path = ObjectPath::parse(s)?;
    Ok(obj_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::path::Path;
    use tempfile::TempDir;

    #[tokio::test]
    async fn simple_create_reopen() {
        logutil::init_test();

        let obj_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();

        let store = LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap();
        let sync = Arc::new(LocalCache::new(store, cache_dir.path().to_path_buf(), 0));

        let path = Path::new("test/file").to_path_buf();
        let mirrored = MirroredFile::create(path.clone(), sync.clone())
            .await
            .unwrap();

        let buf = [0, 1, 2, 3];
        mirrored.write_at(&buf[..], 0).unwrap();
        mirrored.sync_and_remove().await.unwrap();

        let mirrored = MirroredFile::open_from_remote(path, sync.clone())
            .await
            .unwrap();
        let mut read_buf = vec![0; 4];
        mirrored.read_at(&mut read_buf, 0).unwrap();

        assert_eq!(buf[..], read_buf);
    }
}
