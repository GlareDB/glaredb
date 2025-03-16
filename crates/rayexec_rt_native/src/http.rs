use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::{Future, Stream};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_io::http::{HttpClient, HttpResponse};
use reqwest::header::HeaderMap;
use reqwest::{Request, StatusCode};
use tokio::task::JoinHandle;

/// Wrapper around a reqwest client that ensures are request are done in a tokio
/// context.
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
    type Response = BoxingResponse;
    type RequestFuture = ResponseJoinHandle;

    fn do_request(&self, request: Request) -> Self::RequestFuture {
        let fut = self.client.execute(request);
        let join_handle = self.handle.spawn(async move {
            let result = fut.await;

            if result.is_err() {
                println!("ERROR: {result:?}");
            }

            let resp = result.context("Failed to send request")?;

            Ok(BoxingResponse(resp))
        });

        ResponseJoinHandle { join_handle }
    }
}

/// Wrapper around a reqwest response that boxes the futures and streams.
#[derive(Debug)]
pub struct BoxingResponse(pub reqwest::Response);

impl HttpResponse for BoxingResponse {
    type BytesFuture = Pin<Box<dyn Future<Output = Result<Bytes>> + Sync + Send + 'static>>;
    type BytesStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Sync + Send + 'static>>;

    fn status(&self) -> StatusCode {
        self.0.status()
    }

    fn headers(&self) -> &HeaderMap {
        self.0.headers()
    }

    fn bytes(self) -> Self::BytesFuture {
        Box::pin(
            self.0
                .bytes()
                .map(|r| r.context("failed to get byte response")),
        )
    }

    fn bytes_stream(self) -> Self::BytesStream {
        Box::pin(
            self.0
                .bytes_stream()
                .map(|r| r.context("failed to get byte stream")),
        )
    }
}

/// Wrapper around a tokio join handle waiting on a boxed response.
pub struct ResponseJoinHandle {
    join_handle: JoinHandle<Result<BoxingResponse>>,
}

impl Future for ResponseJoinHandle {
    type Output = Result<BoxingResponse>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.join_handle.poll_unpin(cx) {
            Poll::Ready(Err(_)) => Poll::Ready(Err(RayexecError::new("tokio join error"))),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(Ok(b))) => Poll::Ready(Ok(b)),
            Poll::Pending => Poll::Pending,
        }
    }
}
