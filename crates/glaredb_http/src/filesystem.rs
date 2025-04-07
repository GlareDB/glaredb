use std::marker::PhantomData;
use std::task::{Context, Poll};
use std::{fmt, io};

use bytes::Bytes;
use futures::FutureExt;
use glaredb_core::runtime::filesystem::{File, FileSystemFuture};
use glaredb_error::{DbError, Result};
use reqwest::header::RANGE;
use reqwest::{Method, Request};
use url::Url;

use crate::client::{HttpClient, HttpResponse};

#[derive(Debug)]
pub struct HttpFileSystem<C: HttpClient> {
    _c: PhantomData<C>,
}

impl<C> HttpFileSystem<C>
where
    C: HttpClient,
{
    #[allow(clippy::new_without_default)] // TODO: Honestly probably just globally disable this.
    pub const fn new() -> Self {
        HttpFileSystem { _c: PhantomData }
    }
}

enum ChunkReadState<C: HttpClient> {
    /// We're making the initial request.
    Requesting { req_fut: C::RequestFuture },
    /// We're streaming a new chunk.
    Streaming {
        /// The response we're reading from.
        resp: C::Response,
        /// Future returning the chunk.
        chunk_fut: <C::Response as HttpResponse>::ChunkFuture,
    },
    /// We're reading a chunk.
    Reading {
        resp: C::Response,
        /// Position within the chunk.
        pos: usize,
        /// The chunk.
        chunk: Bytes,
    },
    /// No active request happening.
    None,
}

impl<C> ChunkReadState<C>
where
    C: HttpClient,
{
    const fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

impl<C> fmt::Debug for ChunkReadState<C>
where
    C: HttpClient,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkReadState").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct HttpFileHandle<C: HttpClient> {
    url: Url,
    pos: usize,
    chunk: ChunkReadState<C>,
    len: usize,
    client: C,
}

impl<C> HttpFileSystem<C> where C: HttpClient {}

impl<C> File for HttpFileHandle<C>
where
    C: HttpClient,
{
    fn path(&self) -> &str {
        self.url.as_str()
    }

    fn size(&self) -> usize {
        self.len
    }

    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        if self.chunk.is_none() {
            // Make the initial request.
            let mut request = Request::new(Method::GET, self.url.clone());
            let range = format!("bytes={}-{}", self.pos, self.pos + buf.len() - 1);
            request
                .headers_mut()
                .insert(RANGE, range.try_into().unwrap());

            let req_fut = self.client.do_request(request);
            self.chunk = ChunkReadState::Requesting { req_fut }
        }

        let mut count = 0;

        loop {
            match &mut self.chunk {
                ChunkReadState::Requesting { req_fut } => {
                    let mut resp = match req_fut.poll_unpin(cx)? {
                        Poll::Ready(resp) => resp,
                        Poll::Pending => return Poll::Pending,
                    };

                    let chunk_fut = resp.chunk();
                    self.chunk = ChunkReadState::Streaming { resp, chunk_fut };
                    // Continue...
                }
                ChunkReadState::Streaming { chunk_fut, .. } => {
                    let chunk = match chunk_fut.poll_unpin(cx)? {
                        Poll::Ready(chunk) => chunk,
                        Poll::Pending => {
                            if count > 0 {
                                // If we have a non-zero count, it means we
                                // looped here, and did actually write stuff to
                                // the buffer.
                                self.chunk = ChunkReadState::None; // TODO: Do we need to do this?
                                return Poll::Ready(Ok(count));
                            } else {
                                return Poll::Pending;
                            }
                        }
                    };

                    let chunk = match chunk {
                        Some(chunk) => chunk,
                        None => {
                            // Stream finished.
                            self.chunk = ChunkReadState::None; // TODO: Do we need to do this?
                            return Poll::Ready(Ok(count));
                        }
                    };

                    let resp = match std::mem::replace(&mut self.chunk, ChunkReadState::None) {
                        ChunkReadState::Streaming { resp, .. } => resp,
                        other => unreachable!("{other:?}"),
                    };

                    self.chunk = ChunkReadState::Reading {
                        resp,
                        pos: 0,
                        chunk,
                    }
                    // Continue...
                }
                ChunkReadState::Reading { pos, chunk, .. } => {
                    let out = &mut buf[count..];
                    let rem = &chunk[*pos..];

                    let copy_count = usize::min(out.len(), rem.len());

                    let out = &mut out[..copy_count];
                    let rem = &rem[..copy_count];

                    out.copy_from_slice(rem);

                    // Update the count for this poll, as well as our internal
                    // position.
                    count += copy_count;
                    *pos += copy_count;
                    self.pos += copy_count;

                    if *pos >= chunk.len() {
                        // We've exhuasted this chunk. Get more from the stream.
                        let mut resp =
                            match std::mem::replace(&mut self.chunk, ChunkReadState::None) {
                                ChunkReadState::Reading { resp, .. } => resp,
                                other => unreachable!("{other:?}"),
                            };

                        let chunk_fut = resp.chunk();
                        self.chunk = ChunkReadState::Streaming { resp, chunk_fut };

                        // Go back to requesting the next chunk.
                        continue;
                    } else {
                        // Otherwise return what we have.
                        return Poll::Ready(Ok(count));
                    }
                }
                ChunkReadState::None => {
                    // Nothing more to read, just return whatever we have.
                    // TODO: Shouldn't be possible to reach this in the loop.
                    return Poll::Ready(Ok(count));
                }
            }
        }
    }

    fn poll_write(&mut self, _cx: &mut Context, _buf: &[u8]) -> Poll<Result<usize>> {
        // yet
        Poll::Ready(Err(DbError::new("HttpFileHandle does not support writing")))
    }

    fn poll_seek(&mut self, _cx: &mut Context, seek: io::SeekFrom) -> Poll<Result<()>> {
        // Just drop the chunk for whatever request we already have and set the
        // position.
        self.chunk = ChunkReadState::None;
        match seek {
            io::SeekFrom::Start(count) => self.pos = count as usize,
            io::SeekFrom::End(count) => {
                if count > 0 {
                    // It's legal to seek beyond the end, but the read may fail.
                    self.pos = self.len + (count as usize);
                } else {
                    let count = count.abs();
                    if count as usize > self.len {
                        return Poll::Ready(Err(DbError::new(
                            "Cannot seek to before beginning of file",
                        )));
                    }
                    self.pos = self.len - (count as usize);
                }
            }
            io::SeekFrom::Current(count) => {
                if count > 0 {
                    // Just add to current position, as above, it's legal to seek beyond the end.
                    self.pos += count as usize;
                } else {
                    let count = count.abs();
                    if count as usize + self.pos > self.len {
                        return Poll::Ready(Err(DbError::new(
                            "Cannot seek to before beginning of file",
                        )));
                    }
                    self.pos -= count as usize;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        // yet
        Poll::Ready(Err(DbError::new(
            "HttpFileHandle does not support flushing",
        )))
    }
}
