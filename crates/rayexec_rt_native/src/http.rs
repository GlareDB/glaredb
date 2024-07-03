use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use rayexec_error::{Result, ResultExt};
use rayexec_io::{
    http::{HttpClient, HttpReader, ReqwestClient, ReqwestClientReader},
    AsyncReader,
};
use url::Url;

#[derive(Debug)]
pub struct WrappedReqwestClient {
    pub inner: ReqwestClient,
    pub handle: tokio::runtime::Handle,
}

impl HttpClient for WrappedReqwestClient {
    fn reader(&self, url: Url) -> Box<dyn HttpReader> {
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
}

impl HttpReader for WrappedReqwestClientReader {
    fn content_length(&mut self) -> BoxFuture<Result<usize>> {
        self.inner.content_length().boxed()
    }
}
