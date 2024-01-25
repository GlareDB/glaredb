use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, path::Path, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result,
};
use object_store::{GetOptions, PutOptions, PutResult};
use std::env;
use std::fmt;
use std::fs;
use std::ops::Range;
use tempfile::TempDir;
use tokio::io::AsyncWrite;
use tracing::trace;

/// An object store backed by a temporary directory.
#[derive(Debug)]
pub struct TempObjectStore {
    tmp: TempDir,
    inner: LocalFileSystem,
}

impl TempObjectStore {
    pub fn new() -> std::io::Result<TempObjectStore> {
        // Our bare container image doesn't have a '/tmp' dir on startup (nor
        // does it specify an alternate dir to use via `TMPDIR`).
        //
        // The `TempDir` call below will not attempt to create that directory
        // for us.
        let env_tmp = env::temp_dir();
        trace!(?env_tmp, "ensuring temp dir for TempObjectStore");
        fs::create_dir_all(&env_tmp)?;

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
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(&self, location: &Path, bytes: Bytes, opts: PutOptions) -> Result<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
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

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename_if_not_exists(from, to).await
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
