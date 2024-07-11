use bytes::Bytes;
use futures::{
    future::{BoxFuture, FutureExt, TryFutureExt},
    stream::{BoxStream, StreamExt},
    Stream,
};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_io::{
    http::{HttpClient, ReqwestClient, ReqwestClientReader},
    AsyncReader, FileSource,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::debug;
use url::Url;

#[derive(Debug)]
pub struct WrappedReqwestClient {
    pub inner: ReqwestClient,
}

impl HttpClient for WrappedReqwestClient {
    fn reader(&self, url: Url) -> Box<dyn FileSource> {
        Box::new(WrappedReqwestClientReader {
            inner: self.inner.reader(url),
        })
    }
}

#[derive(Debug)]
pub struct WrappedReqwestClientReader {
    pub inner: ReqwestClientReader,
}

impl AsyncReader for WrappedReqwestClientReader {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        let fut = self.inner.read_range(start, len);
        let fut = unsafe { FakeSendFuture::new(Box::pin(fut)) };
        fut.boxed()
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        debug!(url = %self.inner.url, "http streaming (local stream)");

        // Similar to what we do for the "native" client, but boxes everything
        // local. Needed since no part of the stream can be Send in wasm, it's
        // not sufficient to just `boxed_local` the stream, the inner GET
        // requested needs to be local too.
        let fut =
            self.inner
                .client
                .get(self.inner.url.as_str())
                .send()
                .map(|result| match result {
                    Ok(resp) => Ok(resp
                        .bytes_stream()
                        .map(|result| result.context("failed to stream response"))
                        .boxed_local()),
                    Err(e) => Err(RayexecError::with_source(
                        "Failed to send GET request",
                        Box::new(e),
                    )),
                });

        let stream = fut.try_flatten_stream().boxed_local();

        FakeSendStream { stream }.boxed()
    }
}

impl FileSource for WrappedReqwestClientReader {
    fn size(&mut self) -> BoxFuture<Result<usize>> {
        let fut = self.inner.content_length();
        let fut = unsafe { FakeSendFuture::new(Box::pin(fut)) };
        fut.boxed()
    }
}

/// Implements Send for futures that aren't actually Send.
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
struct FakeSendFuture<O, F: Future<Output = O> + Unpin> {
    fut: F,
}

unsafe impl<O, F: Future<Output = O> + Unpin> Send for FakeSendFuture<O, F> {}

impl<O, F: Future<Output = O> + Unpin> FakeSendFuture<O, F> {
    pub unsafe fn new(fut: F) -> Self {
        FakeSendFuture { fut }
    }
}

impl<O, F: Future<Output = O> + Unpin> Future for FakeSendFuture<O, F> {
    type Output = O;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let fut = &mut self.as_mut().fut;
        fut.poll_unpin(cx)
    }
}

/// Similar to `FakeSendFuture`, this unsafely makes a stream send.
#[derive(Debug)]
struct FakeSendStream<O, S: Stream<Item = O> + Unpin> {
    stream: S,
}

unsafe impl<O, S: Stream<Item = O> + Unpin> Send for FakeSendStream<O, S> {}

impl<O, S: Stream<Item = O> + Unpin> Stream for FakeSendStream<O, S> {
    type Item = O;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
