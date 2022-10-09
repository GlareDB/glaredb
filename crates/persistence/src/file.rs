use crate::errors::{internal, PersistenceError, Result};
use bytes::Buf;
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use scc::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing::{trace, warn};

/// Default buffer size when downloading a file from object storage.
const DEFAULT_OBJECT_BUF_SIZE: usize = 2 * 1024 * 1024;

/// Default buffer size when reading from a file and writing to object storage.
const DEFAULT_FILE_READ_SIZE: usize = 64 * 1024;

/// Local file cache for files stored in object storage.
///
/// When a local a request for a file does not exist locally, the file will be
/// pulled from object storage and placed in the local cache.
///
/// NOTE: This cache currently assumes there are at most two references to a
/// mirrored, the reference in the cache, and some other higher level structure.
#[derive(Debug)]
pub struct DiskCache {
    store: Box<dyn ObjectStore>,
    /// Path to where local files will be kept.
    local_cache_path: PathBuf,
    /// Local files that we're aware of in the cache.
    tracked_files: HashMap<PathBuf, Arc<MirroredFile>>,
}

impl DiskCache {
    /// Create a new disk cache.
    pub fn new<O: ObjectStore>(store: O, local_cache_path: PathBuf) -> Self {
        // TODO: Try to rebuild state from what's already stored on disk?
        DiskCache {
            store: Box::new(store),
            local_cache_path,
            tracked_files: HashMap::new(),
        }
    }

    /// Opens an existing file.
    ///
    /// If the file is not cached locally, it will be downloaded from object
    /// storage.
    #[tracing::instrument(level = "trace", skip_all, fields(relative = ?relative.as_ref()))]
    pub async fn open_file<P: AsRef<Path>>(&self, relative: P) -> Result<Arc<MirroredFile>> {
        let path = relative.as_ref();
        match self.tracked_files.read(path, |_, file| file.clone()) {
            Some(file) => {
                trace!("file found in cache");
                Ok(file)
            }
            None => {
                trace!("retrieving file from object store");
                // See note on `DiskCache`, we're assuming that some higher
                // level structure has responsibility for this one file.
                // Otherwise we may end up overwriting an existing file.
                let file = self.download_file(&relative).await?;
                let file = Arc::new(MirroredFile {
                    local_relative: relative.as_ref().to_path_buf(),
                    local: file,
                });

                self.tracked_files
                    .insert(path.to_path_buf(), file.clone())
                    .map_err(|_| internal!("file already tracked: {:?}", path.to_path_buf()))?;

                Ok(file)
            }
        }
    }

    /// Create a new file.
    ///
    /// This will attempt to create a new file locally. No changes to object
    /// storage are made until the resulting file is synced.
    ///
    /// This does not check to see if the file exists in object store.
    #[tracing::instrument(level = "trace", skip_all, fields(relative = ?relative.as_ref()))]
    pub async fn create_file<P: AsRef<Path>>(&self, relative: P) -> Result<Arc<MirroredFile>> {
        trace!("creating file");
        let file = self.open_local_file(&relative)?;
        let file = Arc::new(MirroredFile {
            local_relative: relative.as_ref().to_path_buf(),
            local: file,
        });
        self.tracked_files
            .insert(relative.as_ref().to_path_buf(), file.clone())
            .map_err(|_| {
                internal!(
                    "file already tracked: {:?}",
                    relative.as_ref().to_path_buf()
                )
            })?;
        Ok(file)
    }

    /// Sync the local file to the remote object store.
    #[tracing::instrument(level = "trace", skip_all, fields(relative = ?relative.as_ref()))]
    pub async fn sync_to_remote<P: AsRef<Path>>(&self, relative: P) -> Result<()> {
        trace!("syncing file");
        let file = self
            .tracked_files
            .read(relative.as_ref(), |_, file| file.clone())
            .ok_or_else(|| internal!("missing file for sync: {:?}", relative.as_ref()))?;

        let obj_relative = to_object_path(&relative)?;
        let (id, mut writer) = self.store.put_multipart(&obj_relative).await?;

        file.local.sync_all()?;
        file.as_ref().seek(io::SeekFrom::Start(0))?;
        // Note we're not using a buffered writer here. Each object store client
        // will internally buffer writes.
        let mut offset = 0;
        let file_len = file.local.metadata()?.len() as usize;
        let mut buf = alloc_buf(DEFAULT_FILE_READ_SIZE);
        while offset < file_len {
            let num_read = file.as_ref().read(&mut buf)?;
            trace!(%num_read, "read file");
            if let Err(e) = writer.write_all(&buf[0..num_read]).await {
                if let Err(e) = self.store.abort_multipart(&obj_relative, &id).await {
                    warn!(?e, %id, "failed to abort multipart upload after failed write");
                    // Note that this is a best effort cleanup. We should not
                    // return the error resulting from the failed abort.
                }
                return Err(e.into());
            }
            offset += num_read;
        }

        if let Err(e) = writer.shutdown().await {
            if let Err(e) = self.store.abort_multipart(&obj_relative, &id).await {
                warn!(?e, %id, "failed to abort multipart upload after failed poll shutdown");
                // Similar to above, want to return the first error.
            }
            return Err(e.into());
        }

        Ok(())
    }

    /// Remove a file from the local cache.
    ///
    /// This **does not** sync the file to object store.
    #[tracing::instrument(level = "trace", skip_all, fields(relative = ?relative.as_ref()))]
    pub async fn remove_local<P: AsRef<Path>>(&self, relative: P) -> Result<()> {
        let (_, file) = self
            .tracked_files
            .remove(relative.as_ref())
            .ok_or_else(|| internal!("missing file to remove: {:?}", relative.as_ref()))?;

        let strong_count = Arc::strong_count(&file);
        if strong_count > 1 {
            warn!(%strong_count, path = ?relative.as_ref(), "file has outstanding references");
        }

        std::mem::drop(file);

        let path = self.local_cache_path.join(relative);
        std::fs::remove_file(path)?;

        Ok(())
    }

    async fn download_file<P: AsRef<Path>>(&self, relative: P) -> Result<File> {
        let file = self.open_local_file(&relative)?;
        let obj_relative = to_object_path(&relative)?;
        let mut writer = io::BufWriter::with_capacity(DEFAULT_OBJECT_BUF_SIZE, file);
        let mut stream = self.store.get(&obj_relative).await?.into_stream();

        // TODO: Possible performance improvements with use tokio's async
        // version of buf writer/file.
        //
        // This may end up altering the api for mirrored file to expose async
        // traits/methods. A reason for not doing this now is the arrow ipc
        // reader/writer acts on `Read`/`Write` sources, so we would have to
        // adapt that.
        //
        // A possible alternative or addition to that is to spin up a separate
        // tokio runtime to handle object storage and io operations.
        while let Some(result) = stream.next().await {
            let mut reader = result?.reader();
            let n = io::copy(&mut reader, &mut writer)?;
            trace!(%n, "copied from stream");
        }

        writer.flush()?;
        // TODO: Try to reuse this buffer.
        let (file, _) = match writer.into_parts() {
            (file, Ok(buf)) => (file, buf),
            _ => return Err(internal!("writer panicked")),
        };

        file.sync_all()?;

        Ok(file)
    }

    fn open_local_file<P: AsRef<Path>>(&self, relative: P) -> Result<File> {
        let path = self.local_cache_path.join(relative);
        trace!(?path, "opening local file");
        if let Some(parent) = path.parent() {
            trace!(?parent, "creating parent directory for local file");
            std::fs::create_dir_all(parent)?;
        }

        // TODO: Possibly direct io and/or io uring. This would require some
        // refactoring.
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        Ok(file)
    }
}

/// A file that is mirrored with a file within some remote object store.
///
/// The directory struct on the local disk mirrors the directory structure with
/// the object store.
// TODO: Track bytes read/written.
pub struct MirroredFile {
    local_relative: PathBuf,
    local: File,
}

impl Read for &MirroredFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut local = &self.local;
        (&mut local).read(buf)
    }
}

impl Seek for &MirroredFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let mut local = &self.local;
        (&mut local).seek(pos)
    }
}

// TODO: Have the mirrored file buffer. This will also allow us to track bytes
// written to the local cache.
impl Write for &MirroredFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!(?buf, "writing");
        let mut local = &self.local;
        (&mut local).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut local = &self.local;
        (&mut local).flush()
    }
}

impl fmt::Debug for MirroredFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MirroredFile")
            .field("relative", &self.local_relative)
            .finish()
    }
}

/// Allocated a buffer with undefined contents.
fn alloc_buf(len: usize) -> Vec<u8> {
    let mut b = Vec::with_capacity(len);
    unsafe { b.set_len(len) }
    b
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

pub mod testutil {
    //! Utility module for creating mirrored files for testing.
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::ops::{Deref, DerefMut};
    use tempfile::TempDir;

    /// Wrapper around a disk cache. Derefs to the underlying disk cache.
    pub struct TestDiskCache {
        pub obj_dir: TempDir,
        pub cache_dir: TempDir,
        pub disk_cache: DiskCache,
    }

    impl Deref for TestDiskCache {
        type Target = DiskCache;
        fn deref(&self) -> &Self::Target {
            &self.disk_cache
        }
    }

    impl DerefMut for TestDiskCache {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.disk_cache
        }
    }

    /// Create a new disk cache using a local object store. Both the object
    /// store and disk cache will be backed by temporary files.
    pub fn new_disk_cache() -> TestDiskCache {
        let obj_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();

        let store = LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap();
        let cache = DiskCache::new(store, cache_dir.path().to_path_buf());

        TestDiskCache {
            obj_dir,
            cache_dir,
            disk_cache: cache,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn simple_create_reopen() {
        logutil::init_test();

        let cache = testutil::new_disk_cache();
        let file = cache.create_file("test/file").await.unwrap();

        let buf = [0, 1, 2, 3];
        file.as_ref().seek(io::SeekFrom::Start(0)).unwrap();
        file.as_ref().write_all(&buf).unwrap();
        std::mem::drop(file);

        cache.sync_to_remote("test/file").await.unwrap();
        cache.remove_local("test/file").await.unwrap();

        let file = cache.open_file("test/file").await.unwrap();
        let mut read_buf = vec![0; 4];
        file.as_ref().seek(io::SeekFrom::Start(0)).unwrap();
        file.as_ref().read_exact(&mut read_buf).unwrap();

        assert_eq!(&buf[..], &read_buf);
    }
}
