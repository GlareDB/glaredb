//TODO remove
#![allow(unused_variables)]
//! On-disk cache for byte ranges from object storage
use crate::errors::{internal, Result};

use std::{
    fmt::{Display, Formatter},
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use moka::{
    future::{Cache, CacheBuilder},
    notification::RemovalCause,
};
use object_store::{
    path::Path as ObjectStorePath, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result as ObjectStoreResult,
};
use tokio::{fs, io::AsyncWrite};
use tracing::{error, trace, warn};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct ObjectCacheKey {
    pub object_store_path: ObjectStorePath,
    /// Byte offset in the object storage file
    offset: usize,
}

const OBJECT_STORE_CACHE_FILE_EXTENTION: &str = "bin";

impl ObjectCacheKey {
    //TODO: Consider changing so generated files are all in base_dir instead of object store path
    //relative to base_dir.
    fn to_filename(&self) -> String {
        format!(
            "{}-{}.{}",
            self.object_store_path, self.offset, OBJECT_STORE_CACHE_FILE_EXTENTION
        )
    }

    fn new(path: &Path, offset: usize) -> Result<Self> {
        let object_store_path = crate::file::to_object_path(path)?;
        Ok(Self {
            object_store_path,
            offset,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
//TODO: consider adding checksum for file
//TODO: consider using Arc<Path> to reduce clone of PathBuf when getting the value
struct ObjectCacheValue {
    /// Local path to file storing this given range of bytes
    pub path: PathBuf,
    /// Length of cached file
    // This is a u32 instead of usize due to moka cache weight returning u32
    length: u32,
}

//TODO: Support encrypting cached data at rest
/// Cache of byte ranges from files stored in object storage.
///
/// When a local a request for a byte range from a file does not exist locally, the byte range will
/// be pulled from object storage and placed in the local cache.
#[derive(Debug)]
pub struct ObjectStoreCache {
    /// A thread safe concurrent cache (see `moka` crate)
    cache: Cache<ObjectCacheKey, ObjectCacheValue>,
    /// Path to store the cached byte ranges as files
    base_dir: PathBuf,
    /// Number of bytes cached per range
    byte_range_size: usize,
    /// Total Cache Size, number of cached ranges in bytes
    max_cache_size: u64,
    object_store: Box<dyn ObjectStore>,
}

impl Display for ObjectStoreCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "path to cached byte ranges {}", self.base_dir.display())
    }
}

/// Default size of cached ranges from the object file
const DEFAULT_BYTE_RANGE_SIZE: usize = 4096;
const OBJECT_STORE_CACHE_NAME: &str = "Object Storage Cache";

impl ObjectStoreCache {
    pub fn new(
        cache_path: &Path,
        max_byte_range_size: usize,
        max_cache_size: u64,
        object_store: Box<dyn ObjectStore>,
    ) -> Result<Self> {
        //TODO: Build with_initial_capacity in the future
        //TODO: Investigate using a better hasher for strings/paths in the key
        let cache = CacheBuilder::new(max_cache_size)
            .name("OBJECT_STORE_CACHE_NAME")
            .weigher(Self::weight)
            .support_invalidation_closures() // Allows us to invalidate all ranges for a given object storage file
            //TODO switch to async variant and remove sync
            .eviction_listener_with_queued_delivery_mode(Self::sync_eviction_listener)
            .build();

        // TODO: Is this restriction correct?
        if cache_path.is_relative() {
            return Err(internal!("path must be absolute: {:?}", cache_path));
        }

        Ok(Self {
            cache,
            base_dir: cache_path.to_path_buf(),
            byte_range_size: max_byte_range_size,
            max_cache_size,
            object_store,
        })
    }

    //TODO: expand and remove file on evict
    fn async_eviction_listener(
        key: Arc<ObjectCacheKey>,
        value: ObjectCacheValue,
        removal_cause: RemovalCause,
    ) {
        trace!(?key, ?value, ?removal_cause, "Entry is being evicted");

        let async_runtime = tokio::runtime::Handle::current();
        let _guard = async_runtime.enter();
        async_runtime.block_on(async {
            // Logging on error without propagating to prevent panicking the eviction listener
            if let Err(e) = Self::remove_cache_file(&value).await {
                error!(
                    ?key,
                    ?value,
                    ?removal_cause,
                    "Failed to remove cached object store byte range"
                );
            }
        });
    }

    //TODO: expand and remove file on evict
    fn sync_eviction_listener(
        key: Arc<ObjectCacheKey>,
        value: ObjectCacheValue,
        removal_cause: RemovalCause,
    ) {
        trace!(?key, ?value, ?removal_cause, "Entry is being evicted");

        if let Err(e) = Self::remove_cache_file_sync(&value) {
            error!(
                ?key,
                ?value,
                ?removal_cause,
                "Failed to remove cached object store byte range"
            );
        }
    }

    /// The weight of the cache keys in bytes. This is a heuristic and does not account for all
    /// disk space used such as those used by directories or other meta data about each cache file
    /// beyond the cached data
    fn weight(key: &ObjectCacheKey, value: &ObjectCacheValue) -> u32 {
        value.length
    }

    // TODO: Use async write api vs sync write api from tokio
    // TODO: What permissions should the cache files have
    async fn write_cache_file(
        &self,
        key: &ObjectCacheKey,
        contents: &Bytes,
    ) -> Result<ObjectCacheValue> {
        // Generate a unique file path.
        let path = self.base_dir.join(key.to_filename());

        if path.try_exists()? {
            warn!(?path, "duplicate file being cached");
            return Err(internal!("Cached file path already exists"));
        }

        // We have got a unique file path, so create the file at
        // the path and write the contents to the file.
        let parent = path
            .parent()
            .expect("base_dir is absolute so parent must be Some");
        fs::create_dir_all(parent).await?;
        fs::write(&path, contents).await?;
        trace!(?path, ?parent, ?key, "Cached new file");

        Ok(ObjectCacheValue {
            path,
            length: contents.len() as u32,
        })
    }

    async fn read_cache_file(value: &ObjectCacheValue) -> Result<Bytes> {
        let contents: Bytes = fs::read(&value.path).await?.into();

        if value.length as usize == contents.len() {
            Ok(contents)
        } else {
            Err(internal!("cache file not same length as written"))
        }
    }

    //TODO: currently we are leaking intermediate directories there is no more data
    //TODO: Should this validate a checksum/length?
    async fn remove_cache_file(value: &ObjectCacheValue) -> Result<()> {
        fs::remove_file(&value.path).await?;
        Ok(())
    }

    //TODO: currently we are leaking intermediate directories there is no more data
    //TODO: Should this validate a checksum/length?
    fn remove_cache_file_sync(value: &ObjectCacheValue) -> Result<()> {
        std::fs::remove_file(&value.path)?;
        Ok(())
    }
}

//TODO: Implement ObjectStore trait on ObjectStoreCache
#[async_trait]
impl ObjectStore for ObjectStoreCache {
    async fn put(&self, location: &ObjectStorePath, bytes: Bytes) -> ObjectStoreResult<()> {
        todo!()
    }

    async fn put_multipart(
        &self,
        location: &ObjectStorePath,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        todo!()
    }

    async fn abort_multipart(
        &self,
        location: &ObjectStorePath,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        todo!()
    }

    async fn get(&self, location: &ObjectStorePath) -> ObjectStoreResult<GetResult> {
        todo!()
    }

    async fn get_range(
        &self,
        location: &ObjectStorePath,
        range: Range<usize>,
    ) -> ObjectStoreResult<Bytes> {
        todo!()
    }

    async fn get_ranges(
        &self,
        location: &ObjectStorePath,
        ranges: &[Range<usize>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        todo!()
    }

    async fn head(&self, location: &ObjectStorePath) -> ObjectStoreResult<ObjectMeta> {
        todo!()
    }

    async fn delete(&self, location: &ObjectStorePath) -> ObjectStoreResult<()> {
        todo!()
    }

    async fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        todo!()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> ObjectStoreResult<ListResult> {
        todo!()
    }

    async fn copy(&self, from: &ObjectStorePath, to: &ObjectStorePath) -> ObjectStoreResult<()> {
        todo!()
    }

    async fn rename(&self, from: &ObjectStorePath, to: &ObjectStorePath) -> ObjectStoreResult<()> {
        self.copy(from, to).await?;
        self.delete(from).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> ObjectStoreResult<()> {
        todo!()
    }

    async fn rename_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> ObjectStoreResult<()> {
        self.copy_if_not_exists(from, to).await?;
        self.delete(from).await
    }
}

/// Utility module for creating object store cache for testing
#[cfg(test)]
pub mod test_util {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    pub fn new_object_cache(max_cache_size: u64) -> (ObjectStoreCache, TempDir, TempDir) {
        let obj_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();

        let store = LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap();
        let cache = ObjectStoreCache::new(
            cache_dir.path(),
            DEFAULT_BYTE_RANGE_SIZE,
            max_cache_size,
            Box::new(store),
        )
        .unwrap();
        (cache, obj_dir, cache_dir)
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use super::*;
    use crate::errors::PersistenceError;

    use moka::future::ConcurrentCacheExt;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    #[tokio::test]
    async fn write_cache_file() {
        logutil::init_test();

        let (cache, _, cache_dir) = test_util::new_object_cache(50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = PathBuf::from("test/table1/partition1.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = DEFAULT_BYTE_RANGE_SIZE;

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();
        let val = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();

        cache.cache.insert(key, val.clone()).await;
        let test_obj_store_file = PathBuf::from("test/table1/partition1.parquet");
        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();

        let read_data_serialized = std::fs::read(&val.path).unwrap();
        let read_string = String::from_utf8(read_data_serialized.clone()).unwrap();

        println!("Contents of the file: {}", &read_string);

        assert_eq!(test_data_serialized.len(), read_data_serialized.len());
        assert_eq!(test_data.len(), read_string.len());
        assert_eq!(test_data_serialized, Bytes::from(read_data_serialized));
        assert_eq!(test_data, read_string);
    }

    #[tokio::test]
    async fn duplicate_cache_file() {
        logutil::init_test();

        let (cache, _, cache_dir) = test_util::new_object_cache(50);
        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = PathBuf::from("test/table1/partition1.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = DEFAULT_BYTE_RANGE_SIZE;

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();

        let val = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();

        cache.cache.insert(key.clone(), val.clone()).await;

        let val = cache.write_cache_file(&key, &test_data_serialized).await;

        assert!(
            matches!(val.unwrap_err(), PersistenceError::Internal(e) if e == "Cached file path already exists")
        );
    }

    /// Tests if the given cache directory is a relative path instead of absolute as there is no
    /// default base path. This could change if we can reference a base path from a config
    #[tokio::test]
    async fn relative_cache_dir() {
        logutil::init_test();

        let obj_dir = TempDir::new().unwrap();
        let cache_dir = Path::new("test/dir");

        let store = LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap();
        let cache = ObjectStoreCache::new(cache_dir, DEFAULT_BYTE_RANGE_SIZE, 2, Box::new(store));

        assert!(
            matches!(cache.unwrap_err(), PersistenceError::Internal(e) if e == format!("path must be absolute: {:?}", cache_dir))
        );
    }

    #[tokio::test]
    async fn read_cache_file() {
        logutil::init_test();

        let (cache, _, cache_dir) = test_util::new_object_cache(50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = PathBuf::from("test/table1/partition1.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = DEFAULT_BYTE_RANGE_SIZE;

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();

        // Insert and check valid key
        let val = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();

        cache.cache.insert(key, val.clone()).await;

        let read_data_serialized = ObjectStoreCache::read_cache_file(&val).await.unwrap();
        let read_string = String::from_utf8(read_data_serialized.to_vec()).unwrap();

        println!("Contents of the file: {}", &read_string);

        assert_eq!(test_data_serialized.len(), read_data_serialized.len());
        assert_eq!(test_data.len(), read_string.len());
        assert_eq!(test_data_serialized, Bytes::from(read_data_serialized));
        assert_eq!(test_data, read_string);
    }

    #[tokio::test]
    async fn invalid_cache_file() {
        let (cache, _, _) = test_util::new_object_cache(50);
        let invalid_val = ObjectCacheValue {
            path: PathBuf::from("invalid_path"),
            length: 0,
        };
        let read_data_serialized = ObjectStoreCache::read_cache_file(&invalid_val).await;

        assert!(matches!(
            read_data_serialized.unwrap_err(),
            PersistenceError::Io(_)
        ));
    }

    // TODO:
    // test explicit eviction
    // test eviction due to size
    // test other types of eviction
    // test reading from cache

    #[tokio::test]
    async fn weight() {
        logutil::init_test();

        let (cache, _, cache_dir) = test_util::new_object_cache(50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = PathBuf::from("test/table1/partition1.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = DEFAULT_BYTE_RANGE_SIZE;

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();
        let val = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();
        cache.cache.insert(key, val.clone()).await;

        let test_obj_store_file = PathBuf::from("test/table1/partition2.parquet");

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();
        let val = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();
        cache.cache.insert(key, val.clone()).await;

        cache.cache.sync();

        assert_eq!(cache.cache.entry_count() as usize, 2);
        assert_eq!(
            cache.cache.weighted_size() as usize,
            test_data_serialized.len() * 2
        );
    }

    #[tokio::test]
    async fn invalidate() {
        logutil::init_test();

        let (cache, _, cache_dir) = test_util::new_object_cache(50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = PathBuf::from("test/table1/partition1.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = DEFAULT_BYTE_RANGE_SIZE;

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();
        let val = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();
        cache.cache.insert(key.clone(), val.clone()).await;
        cache.cache.invalidate(&key).await;

        cache.cache.sync();
        assert_eq!(cache.cache.get(&key), None);

        // Check if file is asynchronously cleaned up by eviction listener; Timeout after 60 s
        let mut time = 0;
        while val.path.try_exists().unwrap() && time < 60 {
            sleep(Duration::from_secs(1));
            time += 1;
        }
        assert_eq!(val.path.try_exists().unwrap(), false);
    }
}
