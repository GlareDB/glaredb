use bytes::Bytes;
use futures::{
    future::{BoxFuture, FutureExt, TryFutureExt},
    stream::{BoxStream, StreamExt},
};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_io::{
    http::{HttpClient, ReqwestClient, ReqwestClientReader},
    {AsyncReader, FileSource},
};
use tracing::debug;
use url::Url;

#[derive(Debug)]
pub struct WrappedReqwestClient {
    pub inner: ReqwestClient,
    pub handle: tokio::runtime::Handle,
}

impl HttpClient for WrappedReqwestClient {
    fn reader(&self, url: Url) -> Box<dyn FileSource> {
        Box::new(WrappedReqwestClientReader {
            inner: self.inner.reader(url),
            handle: self.handle.clone(),
        }) as _
    }
}

#[derive(Debug)]
pub struct WrappedReqwestClientReader {
    pub inner: ReqwestClientReader,
    pub handle: tokio::runtime::Handle,
}

impl WrappedReqwestClientReader {
    pub async fn read_range_inner(&mut self, start: usize, len: usize) -> Result<Bytes> {
        let mut inner = self.inner.clone();
        self.handle
            .spawn(async move { inner.read_range(start, len).await })
            .await
            .context("join error")?
    }
}

impl AsyncReader for WrappedReqwestClientReader {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        self.read_range_inner(start, len).boxed()
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        debug!(url = %self.inner.url, "http streaming (send stream)");

        // Folds the initial GET request into the stream.
        let fut =
            self.inner
                .client
                .get(self.inner.url.as_str())
                .send()
                .map(|result| match result {
                    Ok(resp) => Ok(resp
                        .bytes_stream()
                        .map(|result| result.context("failed to stream response"))
                        .boxed()),
                    Err(e) => Err(RayexecError::with_source(
                        "Failed to send GET request",
                        Box::new(e),
                    )),
                });

        fut.try_flatten_stream().boxed()
    }
}

impl FileSource for WrappedReqwestClientReader {
    fn size(&mut self) -> BoxFuture<Result<usize>> {
        self.inner.content_length().boxed()
    }
}
