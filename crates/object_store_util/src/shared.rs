use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path, Error as ObjectStoreError, GetResult, ListResult, ObjectMeta, ObjectStore, Result,
};
use object_store::{GetOptions, MultipartId};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;

/// Implements the object store trait on top of Arc.
///
/// This is useful for the delta lake crate since it expects that the object
/// store points to the root of the _table_. `PrefixStore` can be used for that
/// case, but it requires that the inner store implements `ObjectStore`.
///
/// In most cases, we don't want to recreated the storage client, so we need
/// something cheaply cloneable. That's what this is for.
#[derive(Debug, Clone)]
pub struct SharedObjectStore {
    pub inner: Arc<dyn ObjectStore>,
}

impl SharedObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        SharedObjectStore { inner }
    }
}

impl std::fmt::Display for SharedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SharedObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SharedObjectStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.inner.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        match self.inner.copy_if_not_exists(from, to).await {
            Ok(_) => Ok(()),
            Err(ObjectStoreError::NotSupported { .. }) => {
                // Go with the poor man's copy-if-not-exists: try a regular rename if the path doesn't exist
                match self.head(to).await {
                    Ok(_) => return Err(ObjectStoreError::AlreadyExists {
                        path: to.to_string(),
                        source: anyhow!(
                            "Object at path {to} already exists, can't perform copy-if-not-exists"
                        )
                        .into(),
                    }),
                    Err(ObjectStoreError::NotFound { .. }) => self.copy(from, to).await,
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to).await
    }
}
