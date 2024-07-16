use std::convert;

use bytes::Bytes;
use futures::{
    future::{BoxFuture, FutureExt, TryFutureExt},
    stream::{BoxStream, StreamExt},
    Stream,
};
use rayexec_error::{Result, ResultExt};
use rayexec_io::{http::ReqwestClientReader, FileSource};
use tracing::debug;

#[derive(Debug)]
pub struct WrappedReqwestClientReader {
    pub inner: ReqwestClientReader,
    pub handle: tokio::runtime::Handle,
}

impl WrappedReqwestClientReader {
    async fn read_range_inner(&mut self, start: usize, len: usize) -> Result<Bytes> {
        let mut inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.read_range(start, len).await })
            .await
            .context("join error")?
    }

    async fn create_read_stream(
        handle: tokio::runtime::Handle,
        inner: ReqwestClientReader,
    ) -> Result<impl Stream<Item = Result<Bytes>>> {
        let response = handle
            .spawn(async move {
                inner
                    .client
                    .get(inner.url.as_str())
                    .send()
                    .await
                    .context("Make get request")
            })
            .await
            .context("join handle")
            .and_then(convert::identity)?;

        let stream = response
            .bytes_stream()
            .map(|result| result.context("failed to stream response"));

        Ok(stream)
    }
}

impl FileSource for WrappedReqwestClientReader {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        self.read_range_inner(start, len).boxed()
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        debug!(url = %self.inner.url, "http streaming (send stream)");

        // Folds the initial GET request into the stream.
        let inner = self.inner.clone();
        let fut = Self::create_read_stream(self.handle.clone(), inner);

        fut.try_flatten_stream().boxed()
    }

    fn size(&mut self) -> BoxFuture<Result<usize>> {
        self.inner.content_length().boxed()
    }
}
