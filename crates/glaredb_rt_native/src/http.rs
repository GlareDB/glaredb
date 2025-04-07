use std::pin::Pin;

use bytes::Bytes;
use futures::{Future, Stream, TryStreamExt};
use glaredb_error::{DbError, Result, ResultExt};
use glaredb_http::client::{HttpClient, HttpResponse};
use reqwest::header::HeaderMap;
use reqwest::{Request, StatusCode};

/// Wrapper around a reqwest client that ensures all requests are done in a
/// tokio context.
#[derive(Debug, Clone)]
pub struct TokioWrappedHttpClient {
    client: reqwest::Client,
    handle: tokio::runtime::Handle,
}

impl TokioWrappedHttpClient {
    pub fn new(client: reqwest::Client, handle: tokio::runtime::Handle) -> Self {
        TokioWrappedHttpClient { client, handle }
    }
}

impl HttpClient for TokioWrappedHttpClient {
    type Response = TokioWrappedResponse;
    type RequestFuture =
        Pin<Box<dyn Future<Output = Result<Self::Response>> + Sync + Send + 'static>>;

    fn do_request(&self, request: Request) -> Self::RequestFuture {
        let fut = self.client.execute(request);
        let handle = self.handle.clone();

        Box::pin(async move {
            let join = handle.spawn(fut).await.context("Failed to join")?;
            let resp = join.context("Failed to send request")?;

            Ok(TokioWrappedResponse {
                response: resp,
                handle,
            })
        })
    }
}

/// Wrapper around a reqwest response.
#[derive(Debug)]
pub struct TokioWrappedResponse {
    response: reqwest::Response,
    #[allow(unused)] // TODO: Might possibly need this, don't know yet.
    handle: tokio::runtime::Handle,
}

impl HttpResponse for TokioWrappedResponse {
    type BytesStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Sync + Send + 'static>>;

    fn status(&self) -> StatusCode {
        self.response.status()
    }

    fn headers(&self) -> &HeaderMap {
        self.response.headers()
    }

    fn into_bytes_stream(self) -> Self::BytesStream {
        let stream = self
            .response
            .bytes_stream()
            .map_err(|e| DbError::with_source("Failed to stream body", Box::new(e)));

        Box::pin(stream)
    }
}
