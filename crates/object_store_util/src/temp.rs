use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, path::Path, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result,
};
use std::fmt;
use std::ops::Range;
use tempfile::TempDir;
use tokio::io::AsyncWrite;

/// An object store backed by a temporary directory.
#[derive(Debug)]
pub struct TempObjectStore {
    tmp: TempDir,
    inner: LocalFileSystem,
}

impl TempObjectStore {
    pub fn new() -> std::io::Result<TempObjectStore> {
        let tmp = TempDir::new()?;
        let inner = LocalFileSystem::new_with_prefix(tmp.path())?;
        Ok(TempObjectStore { tmp, inner })
    }

    pub fn local_path(&self) -> &std::path::Path {
        self.tmp.path()
    }
}

#[async_trait]
impl ObjectStore for TempObjectStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.inner.put(location, bytes).await?;
        Ok(())
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let pair = self.inner.put_multipart(location).await?;
        Ok(pair)
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.abort_multipart(location, multipart_id).await?;
        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let result = self.inner.get(location).await?;
        Ok(result)
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let bs = self.inner.get_range(location, range).await?;
        Ok(bs)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let bs = self.inner.get_ranges(location, ranges).await?;
        Ok(bs)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let meta = self.inner.head(location).await?;
        Ok(meta)
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await?;
        Ok(())
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let meta = self.inner.list(prefix).await?;
        Ok(meta)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let result = self.inner.list_with_delimiter(prefix).await?;
        Ok(result)
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await?;
        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to).await?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await?;
        Ok(())
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename_if_not_exists(from, to).await?;
        Ok(())
    }
}

impl fmt::Display for TempObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TempObjectStore(dir={:?}, inner={})",
            self.tmp.path(),
            self.inner
        )
    }
}
