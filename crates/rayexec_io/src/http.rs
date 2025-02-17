use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Future, FutureExt, Stream, StreamExt};
use rayexec_error::{RayexecError, Result, ResultExt};
pub use reqwest;
use reqwest::header::{HeaderMap, RANGE};
use reqwest::{Method, Request, StatusCode};
use serde::de::DeserializeOwned;
use tracing::debug;
use url::Url;

use crate::exp::{AsyncReadStream, FileSource};

pub trait HttpClient: Sync + Send + Debug + Clone {
    type Response: HttpResponse + Send;
    type RequestFuture: Future<Output = Result<Self::Response>> + Send + Unpin;

    fn do_request(&self, request: Request) -> Self::RequestFuture;
}

pub trait HttpResponse {
    type BytesFuture: Future<Output = Result<Bytes>> + Send + Unpin;
    type BytesStream: Stream<Item = Result<Bytes>> + Send + Unpin;

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
    serde_json::from_slice(&full).context("failed to parse response as json")
}

#[derive(Debug)]
pub struct HttpFile<C: HttpClient> {
    client: C,
    url: Url,
}

impl<C> HttpFile<C>
where
    C: HttpClient,
{
    pub fn new(client: C, url: Url) -> Self {
        HttpFile { client, url }
    }
}

impl<C> FileSource for HttpFile<C>
where
    C: HttpClient + Unpin + 'static,
{
    fn read(&mut self) -> Pin<Box<dyn AsyncReadStream>> {
        debug!(url = %self.url, "http read");
        let request = Request::new(Method::GET, self.url.clone());
        let fut = self.client.do_request(request);

        Box::pin(HttpRead::<C>::Requesting {
            fut,
            expected_status: StatusCode::OK,
        })
    }

    fn read_range(&mut self, start: usize, len: usize) -> Pin<Box<dyn AsyncReadStream>> {
        debug!(url = %self.url, %start, %len, "http reading range");
        let range = format_range_header(start, start + len - 1);
        let mut request = Request::new(Method::GET, self.url.clone());
        request
            .headers_mut()
            .insert(RANGE, range.try_into().unwrap());
        let fut = self.client.do_request(request);

        Box::pin(HttpRead::<C>::Requesting {
            fut,
            expected_status: StatusCode::PARTIAL_CONTENT,
        })
    }
}

struct BufferedBytes {
    offset: usize,
    bytes: Bytes,
}

enum HttpRead<C: HttpClient> {
    Requesting {
        fut: C::RequestFuture,
        expected_status: StatusCode,
    },
    Streaming {
        stream: <C::Response as HttpResponse>::BytesStream,
        buffered: Option<BufferedBytes>,
    },
}

impl<C> AsyncReadStream for HttpRead<C>
where
    C: HttpClient + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Result<Poll<Option<usize>>> {
        let this = &mut *self;

        loop {
            match this {
                Self::Requesting {
                    fut,
                    expected_status,
                } => match fut.poll_unpin(cx) {
                    Poll::Ready(Ok(resp)) => {
                        if resp.status() != *expected_status {
                            return Err(RayexecError::new("Response status not what we expected")
                                .with_field("status", resp.status().to_string())
                                .with_field("expected", expected_status.to_string()));
                        }

                        let stream = resp.bytes_stream();
                        *this = HttpRead::Streaming {
                            stream,
                            buffered: None,
                        };
                        continue;
                    }
                    Poll::Ready(Err(err)) => return Err(err),
                    Poll::Pending => return Ok(Poll::Pending),
                },
                Self::Streaming { stream, buffered } => {
                    if buffered.is_none() {
                        // Get bytes from underlying stream.
                        let bytes = match stream.poll_next_unpin(cx) {
                            Poll::Ready(Some(result)) => result?,
                            Poll::Ready(None) => return Ok(Poll::Ready(None)), // Stream complete.
                            Poll::Pending => return Ok(Poll::Pending),
                        };

                        *buffered = Some(BufferedBytes { offset: 0, bytes })
                    }

                    let curr = buffered.as_mut().expect("buffered bytes to be set");

                    let buffered_remaining = curr.bytes.len() - curr.offset;
                    let read_len = usize::min(buffered_remaining, buf.len());

                    let s = &curr.bytes[curr.offset..(curr.offset + read_len)];
                    buf.copy_from_slice(s);

                    if read_len == buffered_remaining {
                        // We've exhausted the current buffered bytes. Next poll
                        // should read from the stream.
                        *buffered = None;
                    }

                    return Ok(Poll::Ready(Some(read_len)));
                }
            }
        }
    }
}

pub(crate) fn format_range_header(start: usize, end: usize) -> String {
    format!("bytes={start}-{end}")
}
