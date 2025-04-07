use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::{Stream, TryStreamExt};
use glaredb_error::{DbError, Result};
use glaredb_http::client::{HttpClient, HttpResponse};
use reqwest::header::HeaderMap;
use reqwest::{Request, StatusCode};

// TODO: The amount of boxing here kinda sucks, but I did the best I could with
// these types. If someone who cares little about their sanity sees this and
// thinks they reduce the boxing while keeping the amount of angle brackets to
// less than 100, go for it. Just make sure it compiles with wasm-pack.

#[derive(Debug, Clone)]
pub struct WasmHttpClient {
    client: reqwest::Client,
}

impl WasmHttpClient {
    #[allow(unused)] // Will take care of this soon
    pub fn new(client: reqwest::Client) -> Self {
        WasmHttpClient { client }
    }
}

impl HttpClient for WasmHttpClient {
    type Response = WasmHttpResponse;
    type RequestFuture =
        Pin<Box<dyn Future<Output = Result<Self::Response>> + Sync + Send + 'static>>;

    fn do_request(&self, request: Request) -> Self::RequestFuture {
        let fut = self.client.execute(request).map(|result| match result {
            Ok(resp) => Ok(WasmHttpResponse(resp)),
            Err(e) => Err(DbError::with_source("Failed to make request", Box::new(e))),
        });
        let fut = unsafe { FakeSyncSendFuture::new(Box::pin(fut)) };
        Box::pin(fut)
    }
}

#[derive(Debug)]
pub struct WasmHttpResponse(reqwest::Response);

/// Same rationale as below for the fake send future/stream.
unsafe impl Send for WasmHttpResponse {}
unsafe impl Sync for WasmHttpResponse {}

impl HttpResponse for WasmHttpResponse {
    type BytesStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Sync + Send + 'static>>;

    fn status(&self) -> StatusCode {
        self.0.status()
    }

    fn headers(&self) -> &HeaderMap {
        self.0.headers()
    }

    fn into_bytes_stream(self) -> Self::BytesStream {
        let stream = self
            .0
            .bytes_stream()
            .map_err(|e| DbError::with_source("Failed to stream body", Box::new(e)));
        let stream = unsafe { FakeSyncSendStream::new(Box::pin(stream)) };
        Box::pin(stream)
    }
}

/// Implements Sync+Send for futures that aren't actually Sync+Send.
///
/// This is needed because the future that we get from reqwest when compiling
/// for wasm is a JsFuture which is not Send. However we make assumptions
/// everywhere in our execution engine that Futures are Send (due to using
/// BoxFuture). To avoid messing with Send vs not-Send, we can use this adapter
/// to unsafely make the JsFuture send.
///
/// This is safe in our case because we're operating on a single thread when
/// running in wasm, and so there's no actual place for our future to be sent
/// to.
///
/// Once we add in web workers for multi-threading, this will still hold because
/// web workers do not transparently share memory, and if we want to send
/// something from one worker to another, we need to do that explicitly via
/// message passing and some sort of serialization.
///
/// I believe there's a way for us to allow the ExecutionRuntime to specify if
/// the implemenation requires Send futures or not, but I (Sean) did not feel
/// like spending the time to figure that out in the initial implemenation.
#[derive(Debug)]
pub struct FakeSyncSendFuture<O, F: Future<Output = O> + Unpin> {
    fut: F,
}

unsafe impl<O, F: Future<Output = O> + Unpin> Send for FakeSyncSendFuture<O, F> {}
unsafe impl<O, F: Future<Output = O> + Unpin> Sync for FakeSyncSendFuture<O, F> {}

impl<O, F: Future<Output = O> + Unpin> FakeSyncSendFuture<O, F> {
    pub unsafe fn new(fut: F) -> Self {
        FakeSyncSendFuture { fut }
    }
}

impl<O, F: Future<Output = O> + Unpin> Future for FakeSyncSendFuture<O, F> {
    type Output = O;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let fut = &mut self.as_mut().fut;
        fut.poll_unpin(cx)
    }
}

/// Similar to `FakeSendFuture`, this unsafely makes a stream send.
#[derive(Debug)]
pub struct FakeSyncSendStream<O, S: Stream<Item = O> + Unpin> {
    stream: S,
}

unsafe impl<O, S: Stream<Item = O> + Unpin> Send for FakeSyncSendStream<O, S> {}
unsafe impl<O, S: Stream<Item = O> + Unpin> Sync for FakeSyncSendStream<O, S> {}

impl<O, S: Stream<Item = O> + Unpin> FakeSyncSendStream<O, S> {
    pub unsafe fn new(stream: S) -> Self {
        FakeSyncSendStream { stream }
    }
}

impl<O, S: Stream<Item = O> + Unpin> Stream for FakeSyncSendStream<O, S> {
    type Item = O;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
