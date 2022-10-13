#![allow(unused_variables)]
//! On-disk cache for byte ranges from object storage
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use moka::future::Cache;
use object_store::path::Path as ObjectStorePath;
use object_store::GetResult;
use object_store::ListResult;
use object_store::MultipartId;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::Result as ObjectStoreResult;
use tokio::io::AsyncWrite;

/// Cache of byte ranges from files stored in object storage.
///
/// When a local a request for a byte range from a file does not exist locally, the byte range will
/// be pulled from object storage and placed in the local cache.
#[derive(Debug)]
pub struct ObjectStoreCache {
    /// A thread safe concurrent cache (see `moka` crate)
    pub cache: Cache<ObjectCacheKey, ObjectCacheValue>,
    /// Path to store the cached byte ranges as files
    path: PathBuf,
    object_store: Arc<dyn ObjectStore>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ObjectCacheKey {
    pub object_store_path: ObjectStorePath,
    /// Byte offset in the object storage file
    offset: usize,
    /// Length of byte range cached
    length: usize,
}

impl ObjectCacheKey {
    pub fn offset(&self) -> usize {
        self.offset
    }
    pub fn length(&self) -> usize {
        self.length
    }
}

#[derive(Debug, Clone)]
//TODO: consider adding checksum for file
//TODO: consider using Arc to reduce clone of PathBuf when getting the value
pub struct ObjectCacheValue {
    /// Local path to file storing this given range of bytes
    pub path: PathBuf,
}

impl Display for ObjectStoreCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "path to cached byte ranges {}", self.path.display())
    }
}

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
