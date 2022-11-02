//! On-disk cache for byte ranges from object storage
use std::{
    fmt::{Display, Formatter},
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use moka::{
    future::{Cache, CacheBuilder},
    notification::RemovalCause,
};
use object_store::{
    path::Path as ObjectStorePath, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result as ObjectStoreResult,
};
use tokio::sync::RwLock;
use tokio::{fs, io::AsyncWrite, runtime::Handle};
use tracing::{debug, error, trace, warn};

use crate::errors::{internal, PersistenceError, Result};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct ObjectCacheKey {
    location: ObjectStorePath,
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
            self.location, self.offset, OBJECT_STORE_CACHE_FILE_EXTENTION
        )
    }

    fn new(location: &str, offset: usize) -> Result<Self> {
        let location = crate::file::to_object_path(location)?;
        Ok(Self { location, offset })
    }
}

#[derive(Debug, Clone)]
//TODO: consider adding checksum for file
//TODO: consider using Arc<Path> to reduce clone of PathBuf when getting the value
struct ObjectCacheValue {
    /// Local path to file storing this given range of bytes
    path: Arc<RwLock<Option<PathBuf>>>,
    /// Length of cached file
    // This is a u32 instead of usize due to moka cache weight returning u32
    length: u32,
}

impl ObjectCacheValue {
    fn new(path: PathBuf, length: u32) -> Self {
        let path = Arc::new(RwLock::new(Some(path)));
        Self { path, length }
    }
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
    object_store: Arc<dyn ObjectStore>,
}

impl Display for ObjectStoreCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ObjectStoreCache(base_dir={}, object_store={})",
            &self.base_dir.display(),
            self.object_store
        )
    }
}

/// Default size of cached ranges from the object file
pub const DEFAULT_BYTE_RANGE_SIZE: usize = 4096;
pub const OBJECT_STORE_CACHE_NAME: &str = "Object Storage Cache";

impl ObjectStoreCache {
    pub fn new(
        cache_path: &Path,
        byte_range_size: usize,
        max_cache_size: u64,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let async_runtime = Handle::current();
        let listener = move |k, v, rc| Self::eviction_listener(&async_runtime, k, v, rc);

        //TODO: Build with_initial_capacity in the future
        //TODO: Investigate using a better hasher for strings/paths in the key
        let cache = CacheBuilder::new(max_cache_size)
            .name(OBJECT_STORE_CACHE_NAME)
            .weigher(Self::weight)
            .support_invalidation_closures() // Allows us to invalidate all ranges for a given object storage file
            .eviction_listener_with_queued_delivery_mode(listener)
            .build();

        // TODO: Is this restriction correct?
        if cache_path.is_relative() {
            return Err(internal!("path must be absolute: {:?}", cache_path));
        }

        Ok(Self {
            cache,
            base_dir: cache_path.to_path_buf(),
            byte_range_size,
            max_cache_size,
            object_store,
        })
    }

    fn eviction_listener(
        async_runtime: &Handle,
        key: Arc<ObjectCacheKey>,
        value: ObjectCacheValue,
        removal_cause: RemovalCause,
    ) {
        trace!(?key, ?value, ?removal_cause, "Entry is being evicted");

        let _guard = async_runtime.enter();
        async_runtime.block_on(async {
            // Logging on error without propagating to prevent panicking the eviction listener
            if let Err(e) = Self::remove_cache_file(&value).await {
                error!(
                    ?e,
                    ?key,
                    ?value,
                    ?removal_cause,
                    "Failed to remove cached object store byte range"
                );
            }
        });
    }

    /// The weight of the cache keys in bytes. This is a heuristic and does not account for all
    /// disk space used such as those used by directories or other meta data about each cache file
    /// beyond the cached data
    fn weight(_key: &ObjectCacheKey, value: &ObjectCacheValue) -> u32 {
        value.length
    }

    // TODO: Use async write api vs sync write api from tokio
    // TODO: What permissions should the cache files have at rest
    async fn write_cache_file(
        &self,
        key: &ObjectCacheKey,
        contents: &Bytes,
    ) -> Result<ObjectCacheValue> {
        // Generate a unique file path.
        let path = self.base_dir.join(key.to_filename());

        if path.try_exists()? {
            //TODO investigate if duplicate file generation is possible when using try_get_with
            warn!(?path, "Duplicate file being cached");
            return Err(PersistenceError::DuplicateCacheFile(path));
        }

        // We have got a unique file path, so create the file at
        // the path and write the contents to the file.
        let parent = path
            .parent()
            .expect("base_dir is absolute so parent must be Some");
        fs::create_dir_all(parent).await?; // TODO: remove once cache files are unique and flat hierarchy
        fs::write(&path, contents).await?;
        let length = contents.len().try_into()?;
        trace!(?path, ?parent, ?key, ?length, "Cached new file");

        Ok(ObjectCacheValue::new(path, length))
    }

    //TODO: Should this validate a checksum/length?
    async fn read_cache_file(value: &ObjectCacheValue) -> Result<Bytes> {
        // If the path is currently write locked we immediately request a retry since the only
        // writer is the eviction_listener
        let path = value
            .path
            .try_read()
            .map_err(|_| PersistenceError::RetryCacheRead)?;

        if let Some(path) = path.as_ref() {
            let contents: Bytes = fs::read(path).await?.into();
            let length = contents.len();
            trace!(?length, ?path, "Read cached file");

            if value.length as usize == length {
                Ok(contents)
            } else {
                Err(internal!("cache file not same length as written"))
            }
        } else {
            // Inidcate a retry is required as eviction has set path to None
            Err(PersistenceError::RetryCacheRead)
        }
    }

    //TODO: currently we are leaking intermediate directories there is no more data
    //TODO: Should this validate a checksum/length?
    async fn remove_cache_file(value: &ObjectCacheValue) -> Result<()> {
        let mut path = value.path.write().await;
        if let Some(p) = path.as_ref() {
            fs::remove_file(p).await?;
            trace!(?p, ?value.length, "Removed cached file");
            *path = None;
        }
        Ok(())
    }

    fn invalidate_location(&self, location: &ObjectStorePath) -> Result<()> {
        let invalidation_path = location.clone();
        if let Err(e) = self
            .cache
            .invalidate_entries_if(move |k, _v| k.location == invalidation_path)
        {
            return Err(internal!(
                "Failed to invalidate cache entries for location {:?}, {:?}",
                location,
                e
            ));
        }
        Ok(())
    }

    async fn get_single_byte_range(
        &self,
        location: &ObjectStorePath,
        offset: usize,
    ) -> Result<Bytes> {
        if offset % self.byte_range_size != 0 {
            return Err(internal!(
                "Offset, {}, is not aligned with byte range {}",
                offset,
                self.byte_range_size
            ));
        }

        let key = ObjectCacheKey {
            location: location.clone(),
            offset,
        };

        // Retry loop if cached entry was invalidated between reading from cache and reading from
        // the file on disk
        // TODO: Consider limiting number of retries to avoid degenerate cases
        loop {
            let value = self
                .cache
                .try_get_with_by_ref(&key, self.init_value(&key))
                .await
                .map_err(|e| {
                    internal!(
                        "Unable to init new cache value key: {:?}, error: {:?}",
                        &key,
                        e
                    )
                })?;

            //TODO: Consider passing a buffer into `init_value` to save save contents from write call
            //and re use that here without re-reading from newly written file
            match Self::read_cache_file(&value).await {
                Ok(contents) => break Ok(contents),
                Err(PersistenceError::RetryCacheRead) => {
                    debug!(?key, ?value, "Retrying read from cache");
                    continue;
                }
                Err(e) => break Err(e),
            }
        }
    }

    async fn init_value(&self, key: &ObjectCacheKey) -> Result<ObjectCacheValue, PersistenceError> {
        let location = &key.location;
        let offset = key.offset;
        //TODO: Save and retrieve meta data within the cache to prevent calling head for every
        //range request
        let metadata = self.object_store.head(location).await?;
        let end = metadata.size.min(offset + self.byte_range_size);
        let range = Range { start: offset, end };

        let contents = self.object_store.get_range(location, range).await?;
        let value = self.write_cache_file(key, &contents).await?;
        Ok(value)
    }
}

#[async_trait]
impl ObjectStore for ObjectStoreCache {
    async fn put(&self, location: &ObjectStorePath, bytes: Bytes) -> ObjectStoreResult<()> {
        self.object_store.put(location, bytes).await?;
        self.invalidate_location(location)?;
        Ok(())
    }

    //TODO: Investigate if this is invalidating the cache even if we abort later, is that something
    //we want to consider avoiding
    async fn put_multipart(
        &self,
        location: &ObjectStorePath,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let write_result = self.object_store.put_multipart(location).await;
        if write_result.is_ok() {
            self.invalidate_location(location)?;
        }
        write_result
    }

    async fn abort_multipart(
        &self,
        location: &ObjectStorePath,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        self.object_store
            .abort_multipart(location, multipart_id)
            .await
    }

    //TODO: consider caching the whole file on get
    async fn get(&self, location: &ObjectStorePath) -> ObjectStoreResult<GetResult> {
        self.object_store.get(location).await
    }

    async fn get_range(
        &self,
        location: &ObjectStorePath,
        range: Range<usize>,
    ) -> ObjectStoreResult<Bytes> {
        trace!(
            ?location,
            ?range,
            "Get range of bytes from object store cache"
        );
        let aligned_range = align_range(&range, self.byte_range_size);
        let first_offset = aligned_range.start;

        let mut aligned_data = BytesMut::with_capacity(aligned_range.end - aligned_range.start);
        //TODO: Run these requests in parallel
        for offset in aligned_range.step_by(self.byte_range_size) {
            let data = self.get_single_byte_range(location, offset).await?;
            //TODO use chain or non copying api on Bytes
            aligned_data.put(data);
        }

        let start = range.start - first_offset;
        let end = range.end - first_offset;
        Ok(aligned_data.freeze().slice(start..end))
    }

    //TODO Decide if we want to cache ObjectMeta in the cache itself
    async fn head(&self, location: &ObjectStorePath) -> ObjectStoreResult<ObjectMeta> {
        self.object_store.head(location).await
    }

    async fn delete(&self, location: &ObjectStorePath) -> ObjectStoreResult<()> {
        self.object_store.delete(location).await?;
        self.invalidate_location(location)?;
        Ok(())
    }

    async fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        self.object_store.list(prefix).await
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> ObjectStoreResult<ListResult> {
        self.object_store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectStorePath, to: &ObjectStorePath) -> ObjectStoreResult<()> {
        self.object_store.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> ObjectStoreResult<()> {
        self.object_store.copy_if_not_exists(from, to).await
    }

    //TODO: Consider optimization to re-insert from location keys with to location
    async fn rename(&self, from: &ObjectStorePath, to: &ObjectStorePath) -> ObjectStoreResult<()> {
        self.object_store.rename(from, to).await?;
        self.invalidate_location(from)?;
        Ok(())
    }

    //TODO: Consider optimization to re-insert from location keys with to location
    async fn rename_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> ObjectStoreResult<()> {
        self.object_store.rename_if_not_exists(from, to).await?;
        self.invalidate_location(from)?;
        Ok(())
    }
}

fn align_range(range: &Range<usize>, alignment: usize) -> Range<usize> {
    let start = range.start - range.start % alignment;
    let end = match range.end % alignment {
        0 => range.end,
        rem => range.end - rem + alignment,
    };
    Range { start, end }
}

/// Utility module for creating object store cache for testing
#[cfg(test)]
pub mod test_util {
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use super::*;

    pub fn new_object_cache(
        byte_range_size: usize,
        max_cache_size: u64,
    ) -> (ObjectStoreCache, TempDir, TempDir) {
        let obj_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();

        let store = LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap();
        let cache = ObjectStoreCache::new(
            cache_dir.path(),
            byte_range_size,
            max_cache_size,
            Arc::new(store),
        )
        .unwrap();
        (cache, obj_dir, cache_dir)
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use futures::future;
    use moka::future::ConcurrentCacheExt;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use super::*;
    use crate::errors::PersistenceError;

    #[tokio::test]
    async fn write_cache_file() {
        logutil::init_test();

        let (cache, _, cache_dir) = test_util::new_object_cache(DEFAULT_BYTE_RANGE_SIZE, 50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = String::from("test/table1/partition1.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = DEFAULT_BYTE_RANGE_SIZE;

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();
        let value = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();

        cache.cache.insert(key, value.clone()).await;

        let path = &*value.path.read().await;
        let path = path.as_ref().unwrap();
        let read_data_serialized = std::fs::read(path).unwrap();
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

        let (cache, _, cache_dir) = test_util::new_object_cache(DEFAULT_BYTE_RANGE_SIZE, 50);
        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = String::from("test/table1/partition2.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = DEFAULT_BYTE_RANGE_SIZE;

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();

        let value = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();

        cache.cache.insert(key.clone(), value.clone()).await;

        let value = cache.write_cache_file(&key, &test_data_serialized).await;

        assert!(matches!(
            value.unwrap_err(),
            PersistenceError::DuplicateCacheFile(_),
        ));
    }

    /// Tests if the given cache directory is a relative path instead of absolute as there is no
    /// default base path. This could change if we can reference a base path from a config
    #[tokio::test]
    async fn relative_cache_dir() {
        logutil::init_test();

        let obj_dir = TempDir::new().unwrap();
        let cache_dir = Path::new("test/dir");

        let store = LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap();
        let cache = ObjectStoreCache::new(cache_dir, DEFAULT_BYTE_RANGE_SIZE, 2, Arc::new(store));

        assert!(
            matches!(cache.unwrap_err(), PersistenceError::Internal(e) if e == format!("path must be absolute: {:?}", cache_dir))
        );
    }

    #[tokio::test]
    async fn read_cache_file() {
        logutil::init_test();

        let (cache, _, cache_dir) = test_util::new_object_cache(DEFAULT_BYTE_RANGE_SIZE, 50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = String::from("test/table1/partition3.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = DEFAULT_BYTE_RANGE_SIZE;

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();

        // Insert and check valid key
        let value = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();

        cache.cache.insert(key, value.clone()).await;

        let read_data_serialized = ObjectStoreCache::read_cache_file(&value).await.unwrap();
        let read_string = String::from_utf8(read_data_serialized.to_vec()).unwrap();

        println!("Contents of the file: {}", &read_string);

        assert_eq!(test_data_serialized.len(), read_data_serialized.len());
        assert_eq!(test_data.len(), read_string.len());
        assert_eq!(test_data_serialized, Bytes::from(read_data_serialized));
        assert_eq!(test_data, read_string);
    }

    #[tokio::test]
    async fn invalid_cache_file() {
        let invalid_val = ObjectCacheValue::new(PathBuf::from("invalid_path"), 0);
        let read_data_serialized = ObjectStoreCache::read_cache_file(&invalid_val).await;

        assert!(matches!(
            read_data_serialized.unwrap_err(),
            PersistenceError::Io(_)
        ));
    }

    #[tokio::test]
    async fn weight() {
        logutil::init_test();

        let (cache, _, cache_dir) = test_util::new_object_cache(DEFAULT_BYTE_RANGE_SIZE, 50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = String::from("test/table1/partition4.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = DEFAULT_BYTE_RANGE_SIZE;

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();
        let value = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();
        cache.cache.insert(key, value.clone()).await;

        let test_obj_store_file = String::from("test/table1/partition5.parquet");

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();
        let value = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();
        cache.cache.insert(key, value.clone()).await;

        cache.cache.sync();

        assert_eq!(cache.cache.entry_count() as usize, 2);
        assert_eq!(
            cache.cache.weighted_size() as usize,
            test_data_serialized.len() * 2
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalidate() {
        logutil::init_test();

        let (cache, _, cache_dir) = test_util::new_object_cache(DEFAULT_BYTE_RANGE_SIZE, 50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = String::from("test/table1/partition6.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = DEFAULT_BYTE_RANGE_SIZE;

        let key = ObjectCacheKey::new(&test_obj_store_file, test_offset).unwrap();
        let value = cache
            .write_cache_file(&key, &test_data_serialized)
            .await
            .unwrap();
        cache.cache.insert(key.clone(), value.clone()).await;
        cache.cache.invalidate(&key).await;

        cache.cache.sync();
        assert!(cache.cache.get(&key).is_none());

        // Check if file is asynchronously cleaned up by eviction listener; Timeout after 1 ms
        let mut nano = 0;
        let path = cache.base_dir.join(key.to_filename());
        while path.try_exists().unwrap() && nano < 1_000_000 {
            nano += 1;
            sleep(Duration::from_nanos(1));
        }
        assert_eq!(path.try_exists().unwrap(), false);
    }

    #[tokio::test]
    async fn align() {
        let alignment = 7;
        let range = Range { start: 5, end: 20 };
        let r = align_range(&range, alignment);
        assert_eq!(r.start, 0);
        assert_eq!(r.end, 21);

        let alignment = 7;
        let range = Range { start: 8, end: 22 };
        let r = align_range(&range, alignment);
        assert_eq!(r.start, 7);
        assert_eq!(r.end, 28);

        let alignment = 7;
        let range = Range { start: 7, end: 22 };
        let r = align_range(&range, alignment);
        assert_eq!(r.start, 7);
        assert_eq!(r.end, 28);

        let alignment = 7;
        let range = Range { start: 8, end: 21 };
        let r = align_range(&range, alignment);
        assert_eq!(r.start, 7);
        assert_eq!(r.end, 21);
    }

    #[tokio::test]
    async fn get_single_byte_range() {
        logutil::init_test();
        let byte_range_size = 2;

        let (cache, _, cache_dir) = test_util::new_object_cache(byte_range_size, 50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = ObjectStorePath::from("test/table1/partition7.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = 6; //offset to "wo" byte range in world

        cache
            .put(&test_obj_store_file, test_data_serialized.clone())
            .await
            .unwrap();

        let data = cache
            .get_single_byte_range(&test_obj_store_file, test_offset)
            .await
            .unwrap();

        println!("Contents of the file: {:?}", &data);

        let range = test_offset..(test_offset + byte_range_size);
        assert_eq!(test_data_serialized.slice(range.clone()), data);
        assert_eq!(
            test_data.get(range.clone()).unwrap(),
            std::str::from_utf8(&data).unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn get_single_byte_range_concurrent() {
        logutil::init_test();
        let byte_range_size = 2;

        let (cache, _, cache_dir) = test_util::new_object_cache(byte_range_size, 50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = ObjectStorePath::from("partition8.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = 6; //offset to "wo" byte range in world

        cache
            .put(&test_obj_store_file, test_data_serialized.clone())
            .await
            .unwrap();

        let mut futs = future::join_all(vec![
            cache.get_single_byte_range(&test_obj_store_file, test_offset),
            cache.get_single_byte_range(&test_obj_store_file, test_offset),
            cache.get_single_byte_range(&test_obj_store_file, test_offset),
            cache.get_single_byte_range(&test_obj_store_file, test_offset),
            cache.get_single_byte_range(&test_obj_store_file, test_offset),
            cache.get_single_byte_range(&test_obj_store_file, test_offset),
            cache.get_single_byte_range(&test_obj_store_file, test_offset),
        ])
        .await;

        let data1 = futs.pop().unwrap().unwrap();
        let data2 = futs.pop().unwrap().unwrap();

        println!("Contents of the file: {:?}", &data1);
        println!("Contents of the file: {:?}", &data2);

        assert_eq!(data1, data2);

        let key = ObjectCacheKey {
            location: test_obj_store_file,
            offset: test_offset,
        };
        let expected_path = cache.base_dir.join(key.to_filename());
        let expected_value = ObjectCacheValue::new(expected_path.clone(), byte_range_size as u32);
        let value = cache.cache.get(&key).unwrap();
        assert_eq!(value.length, expected_value.length);
        assert_eq!(
            &*value.path.read().await,
            &*expected_value.path.read().await
        );

        let paths: Vec<PathBuf> = std::fs::read_dir(cache.base_dir)
            .unwrap()
            .map(|f| f.unwrap().path())
            .collect();

        assert_eq!(paths, vec![expected_path])
    }

    /// Confirms data race between a thread getting and reading a cached file and invalidation:
    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn read_cache_file_invalidate_concurrent() {
        logutil::init_test();
        let byte_range_size = 2;

        let (cache, _, cache_dir) = test_util::new_object_cache(byte_range_size, 50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = ObjectStorePath::from("partition8.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = 4;

        // Put data into object store and populate the cache with one range, key A
        cache
            .put(&test_obj_store_file, test_data_serialized.clone())
            .await
            .unwrap();
        let _ = cache
            .get_single_byte_range(&test_obj_store_file, test_offset)
            .await
            .unwrap();

        // 1. Thread 1: Reads key A from Object Store Cache
        let key = ObjectCacheKey {
            location: test_obj_store_file.clone(),
            offset: test_offset,
        };
        let value = cache.cache.get(&key).unwrap();

        // 2. Thread 2: Put's a new version of an object at the location in A and adds key A to async
        // invalidation queue
        let test_data_2 = "Testing Testing Testing";
        let test_data_serialized_2: Bytes = test_data_2.into();
        cache
            .put(&test_obj_store_file, test_data_serialized_2.clone())
            .await
            .unwrap();

        // 3. Thread 3: Invalidation handler removes cached files associated with object location in key A
        // Wait for path to not exist to confirm invalidation handler completed
        let mut nano = 0;
        let path = cache.base_dir.join(key.to_filename());
        while path.try_exists().unwrap() && nano < 1_000_000 {
            nano += 1;
            sleep(Duration::from_nanos(1));
        }

        // 4. Thread 1: Reads file in location from key A and gets no file exists error
        let error = ObjectStoreCache::read_cache_file(&value).await.unwrap_err();

        trace!(?error);
        assert!(matches!(error, PersistenceError::RetryCacheRead));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn get_single_byte_range_invalidate_concurrent() {
        logutil::init_test();
        let byte_range_size = 2;

        let (cache, _, cache_dir) = test_util::new_object_cache(byte_range_size, 50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = ObjectStorePath::from("partition8.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let test_offset = 4;
        let test_range = Range {
            start: test_offset,
            end: test_offset + byte_range_size,
        };

        cache
            .put(&test_obj_store_file, test_data_serialized.clone())
            .await
            .unwrap();

        let _ = cache
            .get_single_byte_range(&test_obj_store_file, test_offset)
            .await
            .unwrap();

        let test_data_2 = "Testing Testing Testing";
        let test_data_serialized_2: Bytes = test_data_2.into();
        let (res, data2) = future::join(
            cache.put(&test_obj_store_file, test_data_serialized_2.clone()),
            cache.get_range(&test_obj_store_file, test_range.clone()),
        )
        .await;

        let _ = res.unwrap();
        let data2 = data2.unwrap();

        assert_eq!(test_data_serialized.slice(test_range), data2);
    }

    #[tokio::test]
    async fn get_range_object_file() {
        logutil::init_test();
        let byte_range_size = 2;

        let (cache, _, cache_dir) = test_util::new_object_cache(byte_range_size, 50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = ObjectStorePath::from("test/table1/partition9.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let range = 1..7;

        cache
            .put(&test_obj_store_file, test_data_serialized.clone())
            .await
            .unwrap();

        let data = cache
            .get_range(&test_obj_store_file, range.clone())
            .await
            .unwrap();

        println!("Contents of the file: {:?}", &data);

        assert_eq!(test_data_serialized.slice(range.clone()), data);
        assert_eq!(
            test_data.get(range.clone()).unwrap(),
            std::str::from_utf8(&data).unwrap()
        );

        cache.cache.sync();
        let range = align_range(&range, byte_range_size);
        let count = (range.end - range.start) / byte_range_size;
        assert_eq!(cache.cache.entry_count(), count as u64);
    }

    #[tokio::test]
    /// Get last range when left over is less than the byte range size
    async fn get_last_range() {
        logutil::init_test();
        let byte_range_size = 5;

        let (cache, _, cache_dir) = test_util::new_object_cache(byte_range_size, 50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = ObjectStorePath::from("test/table1/partition10.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let range = 6..12;

        cache
            .put(&test_obj_store_file, test_data_serialized.clone())
            .await
            .unwrap();

        let data = cache
            .get_range(&test_obj_store_file, range.clone())
            .await
            .unwrap();

        println!("Contents of the file: {:?}", &data);

        assert_eq!(test_data_serialized.slice(range.clone()), data);
        assert_eq!(
            test_data.get(range.clone()).unwrap(),
            std::str::from_utf8(&data).unwrap()
        );

        cache.cache.sync();
        let range = align_range(&range, byte_range_size);
        let count = (range.end - range.start) / byte_range_size;
        assert_eq!(cache.cache.entry_count(), count as u64);
    }

    #[tokio::test]
    async fn get_ranges_object_file() {
        logutil::init_test();
        let byte_range_size = 2;

        let (cache, _, cache_dir) = test_util::new_object_cache(byte_range_size, 50);

        trace!(?cache_dir, "test cache directory");

        let test_obj_store_file = ObjectStorePath::from("test/table1/partition111.parquet");
        let test_data = "Hello world!";
        let test_data_serialized: Bytes = test_data.into();
        let ranges = [1..6, 5..8];

        cache
            .put(&test_obj_store_file, test_data_serialized.clone())
            .await
            .unwrap();

        let data = cache
            .get_ranges(&test_obj_store_file, &ranges)
            .await
            .unwrap();

        println!("Contents of the file: {:?}", &data);

        let expected_result = vec!["ello ", " wo"];

        assert_eq!(expected_result, data);

        cache.cache.sync();
        assert_eq!(cache.cache.entry_count(), 4);
    }
}
