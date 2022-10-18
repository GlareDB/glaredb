use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path as ObjectPath, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result as ObjectResult,
};
use std::fmt;
use std::ops::Range;
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub struct CachingObjectStore {
    inner: Box<dyn ObjectStore>,
}

impl CachingObjectStore {
    pub fn new(store: Box<dyn ObjectStore>) -> CachingObjectStore {
        CachingObjectStore { inner: store }
    }
}

#[async_trait]
impl ObjectStore for CachingObjectStore {
    async fn put(&self, location: &ObjectPath, bytes: Bytes) -> ObjectResult<()> {
        self.inner.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &ObjectPath,
    ) -> ObjectResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &ObjectPath,
        multipart_id: &MultipartId,
    ) -> ObjectResult<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }

    async fn get(&self, location: &ObjectPath) -> ObjectResult<GetResult> {
        self.inner.get(location).await
    }

    async fn get_range(&self, location: &ObjectPath, range: Range<usize>) -> ObjectResult<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<usize>],
    ) -> ObjectResult<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &ObjectPath) -> ObjectResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectPath) -> ObjectResult<()> {
        self.inner.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectResult<BoxStream<'_, ObjectResult<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> ObjectResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectResult<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectResult<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectResult<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

impl fmt::Display for CachingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Caching({})", self.inner)
    }
}
