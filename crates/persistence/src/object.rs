use crate::errors::{internal, Result};
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use object_store::{path::Path as ObjectPath, ObjectStore};
use scc::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{self, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{trace, warn};

const DEFAULT_BUF_CAP: usize = 2 * 1024 * 1024;

#[derive(Debug)]
pub enum CacheMode {
    /// Cache the downloaded bytes to disk.
    Cache,
    /// Skip caching to disk.
    Skip,
}

impl CacheMode {
    pub fn is_skip(&self) -> bool {
        matches!(self, CacheMode::Skip)
    }
}

pub enum GetResult {
    File(Arc<Mutex<File>>),
    Stream(BoxStream<'static, Result<Bytes>>),
}

pub struct StagedFile {
    pub id: u64,
    pub file: File,
}

/// A wrapper around an object store that can optionally cache files to disk.
///
/// Getting a file can be done via `get`.
///
/// Updating and inserting a file can be accomplished by creating a new staging
/// file via `staged_file`, writing to that file, calling `swap_staged` to bring
/// it into the cache.
///
/// TODO: Currently using tokio's file, but that still requires that it be
/// wrapped in a mutex. Eventually we'll want some sort of native async io.
#[derive(Debug)]
pub struct CachingObjectStore {
    store: Box<dyn ObjectStore>,
    local: PathBuf,
    tracked_objects: HashMap<PathBuf, Arc<Mutex<File>>>,
}

impl CachingObjectStore {
    /// Create a new caching store, caching downloaded objects at the given
    /// path.
    pub fn new<O, P>(store: O, path: P) -> Self
    where
        O: ObjectStore,
        P: AsRef<Path>,
    {
        // TODO: Try to rebuild cache? Remove everything?
        CachingObjectStore {
            store: Box::new(store),
            local: path.as_ref().to_path_buf(),
            tracked_objects: HashMap::new(),
        }
    }

    /// Get an object at some path.
    pub async fn get<P: AsRef<Path>>(&self, path: P, mode: CacheMode) -> Result<GetResult> {
        let path = path.as_ref();
        if let Some(file) = self.tracked_objects.read(path, |_, file| file.clone()) {
            trace!(?path, "using cached file");
            return Ok(GetResult::File(file));
        }

        // TODO: Synchronize download. We may end up overwriting some other
        // incomplete download.

        let file = self.open_local(path).await?;
        let mut writer = io::BufWriter::with_capacity(DEFAULT_BUF_CAP, file);

        let obj_path = to_object_path(path)?;
        let mut stream = self.store.get(&obj_path).await?.into_stream();

        if mode.is_skip() {
            let stream = stream.map(|s| s.map_err(|e| e.into()));
            return Ok(GetResult::Stream(stream.boxed()));
        }

        while let Some(result) = stream.next().await {
            let bytes = result?;
            writer.write_all(&bytes[..]).await?;
        }
        writer.flush().await?;
        let file = Arc::new(Mutex::new(writer.into_inner()));

        self.tracked_objects
            .insert(path.to_path_buf(), file.clone())
            .map_err(|_| internal!("file already exists for path: {:?}", path))?;

        Ok(GetResult::File(file))
    }

    /// Get a file to stage writes to.
    pub async fn staged_file(&self) -> Result<StagedFile> {
        static ID: AtomicU64 = AtomicU64::new(0);
        let id = ID.fetch_add(1, Ordering::Relaxed);
        trace!(%id, "staging with id");

        let staged_path = self.local.join("stage").join(id.to_string());
        if let Some(parent) = staged_path.parent() {
            trace!(?parent, "creating parent directory for staged file");
            fs::create_dir_all(parent).await?;
        }

        trace!(?staged_path, "opening staged file");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&staged_path)
            .await?;

        Ok(StagedFile { id, file })
    }

    /// Swap a staged file into the cache.
    pub async fn swap_staged<P: AsRef<Path>>(
        &self,
        staged: StagedFile,
        path: P,
    ) -> Result<Arc<Mutex<File>>> {
        let staged_path = self.local.join("stage").join(staged.id.to_string());
        let to_path = self.local.join(&path);
        if let Some(parent) = to_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        trace!(id = %staged.id, ?staged_path, ?to_path, "swapping staged");

        staged.file.sync_all().await?;
        fs::rename(staged_path, to_path).await?;

        let file = Arc::new(Mutex::new(staged.file));
        self.tracked_objects.upsert(
            path.as_ref().to_path_buf(),
            || file.clone(),
            |path, file| {
                let count = Arc::strong_count(file);
                if count > 1 {
                    warn!(?path, %count, "strong count for file above one");
                }
                *file = file.clone();
            },
        );

        Ok(file)
    }

    /// Sync a file to the remote object store.
    pub async fn sync_to_remote<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let file = self
            .tracked_objects
            .read(path, |_, file| file.clone())
            .ok_or_else(|| internal!("missing file for upload: {:?}", path))?;

        // TODO: Switch to some othe async io where we don't need to lock.
        let file = file.lock().await;
        let mut file = file.try_clone().await?;
        file.sync_all().await?;
        file.seek(io::SeekFrom::Start(0)).await?;
        let mut reader = io::BufReader::new(file);

        let obj_relative = to_object_path(path)?;
        let (id, mut writer) = self.store.put_multipart(&obj_relative).await?;

        if let Err(e) = io::copy(&mut reader, &mut writer).await {
            if let Err(e) = self.store.abort_multipart(&obj_relative, &id).await {
                warn!(?e, %id, "failed to abort multipart upload after failed write");
                // Note that this is a best effort cleanup. We should not
                // return the error resulting from the failed abort.
            }
            return Err(e.into());
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

    /// Remove a local file.
    ///
    /// Does not attempt to sync the file beforehand.
    pub async fn remove_local<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let (_, file) = self
            .tracked_objects
            .remove(path)
            .ok_or_else(|| internal!("missing file for removal: {:?}", path))?;

        let count = Arc::strong_count(&file);
        if count > 1 {
            warn!(%count, ?path, "strong count for file greater than one");
        }
        let path = self.local.join(path);
        fs::remove_file(path).await?;

        Ok(())
    }

    async fn open_local<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        let path = self.local.join(path);
        trace!(?path, "opening local file");
        if let Some(parent) = path.parent() {
            trace!(?parent, "creating parent directory for local file");
            fs::create_dir_all(parent).await?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .await?;

        Ok(file)
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

pub mod testutil {
    //! Utility module for creating caching object stores for testing
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::ops::{Deref, DerefMut};
    use tempfile::TempDir;

    pub struct TestCachingObjectStore {
        pub obj_dir: TempDir,
        pub cache_dir: TempDir,
        pub obj_cache: CachingObjectStore,
    }

    impl Deref for TestCachingObjectStore {
        type Target = CachingObjectStore;
        fn deref(&self) -> &Self::Target {
            &self.obj_cache
        }
    }

    impl DerefMut for TestCachingObjectStore {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.obj_cache
        }
    }

    /// Create a new disk cache using a local object store. Both the object
    /// store and disk cache will be backed by temporary files.
    pub fn new_obj_cache() -> TestCachingObjectStore {
        let obj_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();

        let store = LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap();
        let cache = CachingObjectStore::new(store, cache_dir.path());

        TestCachingObjectStore {
            obj_dir,
            cache_dir,
            obj_cache: cache,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn simple_create_reopen() {
        logutil::init_test();

        let cache = testutil::new_obj_cache();
        let mut staged = cache.staged_file().await.unwrap();

        let buf = [0, 1, 2, 3];
        staged.file.seek(io::SeekFrom::Start(0)).await.unwrap();
        staged.file.write_all(&buf).await.unwrap();
        staged.file.flush().await.unwrap();

        cache.swap_staged(staged, "test/file").await.unwrap();
        cache.sync_to_remote("test/file").await.unwrap();
        cache.remove_local("test/file").await.unwrap();

        let get = cache.get("test/file", CacheMode::Cache).await.unwrap();
        match get {
            GetResult::File(file) => {
                let mut read_buf = vec![0; 4];
                let mut file = file.lock().await;
                file.seek(io::SeekFrom::Start(0)).await.unwrap();
                file.read_exact(&mut read_buf).await.unwrap();
                assert_eq!(&buf[..], &read_buf);
            }
            _ => {
                panic!("unexpected get result");
            }
        }
    }
}
