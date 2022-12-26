use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path as ObjectPath, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;

/// A wrapper around an object store that prefixes all paths with a constant
/// string.
#[derive(Debug, Clone)]
pub struct PrefixObjectStore {
    prefix: String,
    inner: Arc<dyn ObjectStore>,
}

impl PrefixObjectStore {
    pub fn new(prefix: String, inner: Arc<dyn ObjectStore>) -> PrefixObjectStore {
        PrefixObjectStore { prefix, inner }
    }

    pub fn local_path(&self) -> &str {
        self.prefix.as_str()
    }

    fn prefix_path(&self, path: &ObjectPath) -> ObjectPath {
        ObjectPath::from(format!("{}/{}", self.prefix, path))
    }
}

#[async_trait]
impl ObjectStore for PrefixObjectStore {
    async fn put(&self, location: &ObjectPath, bytes: Bytes) -> Result<()> {
        self.inner.put(&self.prefix_path(location), bytes).await?;
        Ok(())
    }

    async fn put_multipart(
        &self,
        location: &ObjectPath,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let pair = self
            .inner
            .put_multipart(&self.prefix_path(location))
            .await?;
        Ok(pair)
    }

    async fn abort_multipart(
        &self,
        location: &ObjectPath,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        self.abort_multipart(&self.prefix_path(location), multipart_id)
            .await?;
        Ok(())
    }

    async fn get(&self, location: &ObjectPath) -> Result<GetResult> {
        let result = self.inner.get(&self.prefix_path(location)).await?;
        Ok(result)
    }

    async fn get_range(&self, location: &ObjectPath, range: Range<usize>) -> Result<Bytes> {
        let bs = self
            .inner
            .get_range(&self.prefix_path(location), range)
            .await?;
        Ok(bs)
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<usize>],
    ) -> Result<Vec<Bytes>> {
        let bs = self
            .inner
            .get_ranges(&self.prefix_path(location), ranges)
            .await?;
        Ok(bs)
    }

    async fn head(&self, location: &ObjectPath) -> Result<ObjectMeta> {
        let meta = self.inner.head(&self.prefix_path(location)).await?;
        Ok(meta)
    }

    async fn delete(&self, location: &ObjectPath) -> Result<()> {
        self.inner.delete(&self.prefix_path(location)).await?;
        Ok(())
    }

    async fn list(&self, prefix: Option<&ObjectPath>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let meta = match prefix {
            Some(prefix) => self.inner.list(Some(&self.prefix_path(prefix))).await?,
            None => {
                self.inner
                    .list(Some(&ObjectPath::from(self.prefix.clone())))
                    .await?
            }
        };
        Ok(meta)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> Result<ListResult> {
        let result = match prefix {
            Some(prefix) => {
                self.inner
                    .list_with_delimiter(Some(&self.prefix_path(prefix)))
                    .await?
            }
            None => {
                self.inner
                    .list_with_delimiter(Some(&ObjectPath::from(self.prefix.clone())))
                    .await?
            }
        };
        Ok(result)
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> Result<()> {
        self.inner
            .copy(&self.prefix_path(from), &self.prefix_path(to))
            .await?;
        Ok(())
    }

    async fn rename(&self, from: &ObjectPath, to: &ObjectPath) -> Result<()> {
        self.inner
            .rename(&self.prefix_path(from), &self.prefix_path(to))
            .await?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> Result<()> {
        self.inner
            .copy_if_not_exists(&self.prefix_path(from), &self.prefix_path(to))
            .await?;
        Ok(())
    }

    async fn rename_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> Result<()> {
        self.inner
            .rename_if_not_exists(&self.prefix_path(from), &self.prefix_path(to))
            .await?;
        Ok(())
    }
}

impl fmt::Display for PrefixObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PrefixObjectStore(prefix={}, inner={})",
            self.prefix, self.inner
        )
    }
}
