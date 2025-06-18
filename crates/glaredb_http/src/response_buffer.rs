use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use glaredb_core::util::marker::PhantomCovariant;
use glaredb_error::{Result, ResultExt};
use serde::Deserialize;

/// Object for holding the raw buffer and a deserialized response.
///
/// This can be used for avoiding copies during deserialization, as well as
/// buffer reuse.
#[derive(Debug)]
pub struct ResponseBuffer<R> {
    buf: Vec<u8>,
    resp: R,
}

impl<R> ResponseBuffer<R> {
    /// Try to read a json body from a stream.
    ///
    /// The provided buffer's contents will be cleared, then used for buffering
    /// the response.
    pub fn read_stream_json<S>(mut buf: Vec<u8>, stream: S) -> ReadStreamJsonFuture<R, S>
    where
        R: for<'de> Deserialize<'de>,
        S: Stream<Item = Result<Bytes>> + Sync + Send + Unpin,
    {
        buf.clear();
        ReadStreamJsonFuture {
            buf,
            stream,
            _resp: PhantomCovariant::new(),
        }
    }

    pub fn response(&self) -> &R {
        &self.resp
    }

    pub fn into_buffer(self) -> Vec<u8> {
        self.buf
    }
}

pub struct ReadStreamJsonFuture<R, S> {
    buf: Vec<u8>,
    stream: S,
    _resp: PhantomCovariant<R>,
}

impl<R, S> Future for ReadStreamJsonFuture<R, S>
where
    R: for<'de> Deserialize<'de>,
    S: Stream<Item = Result<Bytes>> + Sync + Send + Unpin,
{
    type Output = Result<ResponseBuffer<R>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            match this.stream.poll_next_unpin(cx)? {
                Poll::Ready(Some(bs)) => {
                    this.buf.extend_from_slice(bs.as_ref());
                    // Continue...
                }
                Poll::Ready(None) => {
                    // Stream finished, deserialize.
                    let buf = std::mem::take(&mut this.buf);
                    let resp: R =
                        match serde_json::from_slice(&buf).context("Failed to deserialize json") {
                            Ok(resp) => resp,
                            Err(err) => return Poll::Ready(Err(err)),
                        };
                    return Poll::Ready(Ok(ResponseBuffer { buf, resp }));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
