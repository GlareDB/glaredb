use std::fmt::Debug;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use futures::{Future, Stream, StreamExt, TryStreamExt};
use rayexec_error::{RayexecError, Result, ResultExt};
pub use reqwest;
use reqwest::header::{HeaderMap, CONTENT_LENGTH, RANGE};
use reqwest::{Method, Request, StatusCode};
use serde::de::DeserializeOwned;
use tracing::debug;
use url::Url;

use crate::FileSource;

pub trait HttpClient: Sync + Send + Debug + Clone {
    type Response: HttpResponse + Send;
    type RequestFuture: Future<Output = Result<Self::Response>> + Send;

    fn do_request(&self, request: Request) -> Self::RequestFuture;
}

pub trait HttpResponse {
    type BytesFuture: Future<Output = Result<Bytes>> + Send;
    type BytesStream: Stream<Item = Result<Bytes>> + Send;

    fn status(&self) -> StatusCode;
    fn headers(&self) -> &HeaderMap;
    fn bytes(self) -> Self::BytesFuture;
    fn bytes_stream(self) -> Self::BytesStream;
}

pub async fn read_text(resp: impl HttpResponse) -> Result<String> {
    let full = resp.bytes().await?;
    Ok(String::from_utf8_lossy(&full).to_string())
}

pub async fn read_json<T: DeserializeOwned>(resp: impl HttpResponse) -> Result<T> {
    let full = resp.bytes().await?;

    // let s = str::from_utf8(&full).unwrap();
    // print!("RESP: {s}");

    serde_json::from_slice(&full).context("failed to parse response as json")
}

#[derive(Debug)]
pub struct HttpClientReader<C: HttpClient> {
    client: C,
    url: Url,
}

impl<C: HttpClient> HttpClientReader<C> {
    pub fn new(client: C, url: Url) -> Self {
        HttpClientReader { client, url }
    }
}

impl<C: HttpClient + 'static> FileSource for HttpClientReader<C> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        debug!(url = %self.url, %start, %len, "http reading range");

        let range = format_range_header(start, start + len - 1);

        let mut request = Request::new(Method::GET, self.url.clone());
        request
            .headers_mut()
            .insert(RANGE, range.try_into().unwrap());

        let fut = self.client.do_request(request);

        Box::pin(async move {
            let resp = fut.await?;

            if resp.status() != StatusCode::PARTIAL_CONTENT {
                return Err(RayexecError::new("Server does not support range requests"));
            }

            resp.bytes().await.context("failed to get response body")
        })
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        debug!(url = %self.url, "http reading stream");

        let client = self.client.clone();
        let req = Request::new(Method::GET, self.url.clone());

        let stream = stream::once(async move {
            let resp = client.do_request(req).await?;

            Ok::<_, RayexecError>(resp.bytes_stream())
        })
        .try_flatten();

        stream.boxed()
    }

    fn size(&mut self) -> BoxFuture<Result<usize>> {
        debug!(url = %self.url, "http getting content length");

        let fut = self
            .client
            .do_request(Request::new(Method::GET, self.url.clone()));

        Box::pin(async move {
            let resp = fut.await?;

            if !resp.status().is_success() {
                return Err(RayexecError::new("Failed to get content-length"));
            }

            let len = match resp.headers().get(CONTENT_LENGTH) {
                Some(header) => header
                    .to_str()
                    .context("failed to convert to string")?
                    .parse::<usize>()
                    .context("failed to parse content length")?,
                None => return Err(RayexecError::new("Response missing content-length header")),
            };

            Ok(len)
        })
    }
}

pub(crate) fn format_range_header(start: usize, end: usize) -> String {
    format!("bytes={start}-{end}")
}
