use std::fmt::Debug;
use std::task::{Context, Poll};
use std::{fmt, io};

use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use glaredb_core::runtime::filesystem::FileHandle;
use glaredb_error::{DbError, Result};
use reqwest::header::RANGE;
use reqwest::{Method, Request};
use url::Url;

use crate::client::{HttpClient, HttpResponse};

pub trait RequestSigner: Sync + Send + Debug + 'static {
    fn sign(&self, request: Request) -> Result<Request>;
}

#[derive(Debug)]
pub struct HttpFileHandle<C: HttpClient, S: RequestSigner> {
    pub(crate) url: Url,
    pub(crate) pos: usize,
    pub(crate) chunk: ChunkReadState<C>,
    pub(crate) len: usize,
    pub(crate) client: C,
    pub(crate) signer: S,
}

impl<C, S> HttpFileHandle<C, S>
where
    C: HttpClient,
    S: RequestSigner,
{
    /// Create a new handle represting a file at the given location.
    ///
    /// The initial position will be at the start.
    pub(crate) fn new(url: Url, len: usize, client: C, signer: S) -> Self {
        HttpFileHandle {
            url,
            pos: 0,
            chunk: ChunkReadState::None,
            len,
            client,
            signer,
        }
    }
}

impl<C, S> FileHandle for HttpFileHandle<C, S>
where
    C: HttpClient,
    S: RequestSigner,
{
    fn path(&self) -> &str {
        self.url.as_str()
    }

    fn size(&self) -> usize {
        self.len
    }

    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        let mut count = 0;

        loop {
            match &mut self.chunk {
                ChunkReadState::None => {
                    // Make the initial request.
                    let mut request = Request::new(Method::GET, self.url.clone());
                    // Range always uses the entire buf. We should never be
                    // making the request if count > 0.
                    let range = format!("bytes={}-{}", self.pos, self.pos + buf.len() - 1);
                    request
                        .headers_mut()
                        .insert(RANGE, range.try_into().unwrap());

                    let request = self.signer.sign(request)?;

                    let req_fut = self.client.do_request(request);
                    self.chunk = ChunkReadState::Requesting { req_fut }
                }

                ChunkReadState::Requesting { req_fut } => {
                    let resp = match req_fut.poll_unpin(cx)? {
                        Poll::Ready(resp) => resp,
                        Poll::Pending => return Poll::Pending,
                    };

                    let stream = resp.into_bytes_stream();
                    self.chunk = ChunkReadState::Streaming { stream };
                    // Continue...
                }

                ChunkReadState::Streaming { stream, .. } => {
                    let chunk = match stream.poll_next_unpin(cx)? {
                        Poll::Ready(Some(chunk)) => chunk,
                        Poll::Ready(None) => {
                            // Stream finished.
                            //
                            // Set chunk state to None to trigger new request on
                            // the next poll.
                            self.chunk = ChunkReadState::None;
                            if count == 0 {
                                // If we didn't actually read anything, go ahead
                                // an make the next request.
                                //
                                // This may happen if we already have a chunk,
                                // but we're at the end, and attempt to pull
                                // more from the stream.
                                continue;
                            }

                            // Otherwise return what we have.
                            return Poll::Ready(Ok(count));
                        }
                        Poll::Pending => {
                            // Note this requires that we don't loop on getting
                            // a read. Otherwise we'd end up losing parts of the
                            // chunk.
                            return Poll::Pending;
                        }
                    };

                    let stream = match std::mem::replace(&mut self.chunk, ChunkReadState::None) {
                        ChunkReadState::Streaming { stream, .. } => stream,
                        other => unreachable!("{other:?}"),
                    };

                    self.chunk = ChunkReadState::Reading {
                        stream,
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
                        let stream = match std::mem::replace(&mut self.chunk, ChunkReadState::None)
                        {
                            ChunkReadState::Reading { stream, .. } => stream,
                            other => unreachable!("{other:?}"),
                        };

                        self.chunk = ChunkReadState::Streaming { stream };

                        // Return here with what we have, we'll stream the next
                        // chunk on the next poll.
                        return Poll::Ready(Ok(count));
                    } else {
                        // Otherwise return what we have.
                        //
                        // Keep the chunk stream state, we'll continue reading
                        // from what we have buffered.
                        return Poll::Ready(Ok(count));
                    }
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
                    let count = count.unsigned_abs() as usize;
                    if count > self.pos {
                        return Poll::Ready(Err(DbError::new(
                            "Cannot seek to before beginning of file",
                        )));
                    }
                    self.pos -= count;
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

pub(crate) enum ChunkReadState<C: HttpClient> {
    /// We're making the initial request.
    Requesting { req_fut: C::RequestFuture },
    /// We're streaming a new chunk.
    Streaming {
        /// Stream returning chunks.
        stream: <C::Response as HttpResponse>::BytesStream,
    },
    /// We're reading a chunk.
    Reading {
        stream: <C::Response as HttpResponse>::BytesStream,
        /// Position within the chunk.
        pos: usize,
        /// The chunk.
        chunk: Bytes,
    },
    /// No active request happening.
    None,
}

impl<C> fmt::Debug for ChunkReadState<C>
where
    C: HttpClient,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkReadState").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use futures::{Stream, stream};
    use glaredb_core::util::task::noop_context;
    use reqwest::StatusCode;
    use reqwest::header::HeaderMap;

    use super::*;
    use crate::filesystem::NopRequestSigner;

    /// Server mock implementing `HttpClient` that streams back fixed-sized
    /// chunks.
    #[derive(Debug, Clone)]
    struct FixedSizedStreamer {
        content: Bytes,
        chunk_size: usize,
    }

    impl FixedSizedStreamer {
        fn new(content: impl AsRef<[u8]>, chunk_size: usize) -> Self {
            FixedSizedStreamer {
                content: Bytes::from(content.as_ref().to_vec()),
                chunk_size,
            }
        }

        fn handle(&self) -> HttpFileHandle<Self, NopRequestSigner> {
            let loc = Url::parse("https://bigdatacompany.com/file").unwrap();
            HttpFileHandle::new(loc, self.content.len(), self.clone(), NopRequestSigner)
        }

        fn generate_chunks_from_request(&self, req: &Request) -> Vec<Bytes> {
            let range = req.headers().get(RANGE).expect("RANGE header to exist");
            let range = range.to_str().unwrap();

            let range = range.trim_start_matches("bytes=");
            let (start, end) = range.split_once("-").expect("format: start-end");
            let start = start.parse::<usize>().unwrap();
            let end = end.parse::<usize>().unwrap();

            // 'end' in the range header is inclusive.
            let end = end + 1;

            self.generate_chunks(start, end)
        }

        fn generate_chunks(&self, start: usize, end: usize) -> Vec<Bytes> {
            // Range requests can go beyond the end of the content. Standards
            // conforming servers should return the part that overlaps with the
            // range.
            let end = usize::min(end, self.content.len());

            let mut chunks = Vec::new();
            let mut curr_start = start;

            while curr_start < end {
                let curr_end = (curr_start + self.chunk_size).min(end);
                let chunk = self.content.slice(curr_start..curr_end);
                chunks.push(chunk);
                curr_start = curr_end;
            }
            chunks
        }
    }

    impl HttpClient for FixedSizedStreamer {
        type Response = FixedSizedResponse;
        type RequestFuture = Pin<Box<dyn Future<Output = Result<Self::Response>> + Sync + Send>>;

        fn do_request(&self, request: Request) -> Self::RequestFuture {
            let chunks = self.generate_chunks_from_request(&request);
            Box::pin(async move {
                Ok(FixedSizedResponse {
                    chunks,
                    headers: HeaderMap::new(),
                })
            })
        }
    }

    #[derive(Debug)]
    struct FixedSizedResponse {
        chunks: Vec<Bytes>,
        headers: HeaderMap,
    }

    impl HttpResponse for FixedSizedResponse {
        type BytesStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Sync + Send + Unpin>>;

        fn status(&self) -> StatusCode {
            // TODO: Could mock this out in case a server doesn't support range
            // requests.
            StatusCode::PARTIAL_CONTENT
        }

        fn headers(&self) -> &HeaderMap {
            &self.headers
        }

        fn into_bytes_stream(self) -> Self::BytesStream {
            let chunks = self.chunks.clone();
            Box::pin(stream::iter(chunks.into_iter().map(Ok)))
        }
    }

    #[test]
    fn large_read_buffer_large_chunk_size() {
        // Read from a stream of one chunk.

        let streamer = FixedSizedStreamer::new(b"hello", 10);
        let mut handle = streamer.handle();

        let mut buf = vec![0; 8];
        let poll = handle
            .poll_read(&mut noop_context(), &mut buf)
            .map(|r| r.unwrap());

        assert_eq!(Poll::Ready(5), poll);
        assert_eq!(b"hello", &buf[0..5]);
    }

    #[test]
    fn small_read_buffer_large_chunk_size() {
        // Read from a stream of one chunk into a small buffer.
        //
        // We should continue to be able to poll to get the rest of the chunk.

        let streamer = FixedSizedStreamer::new(b"hello", 10);
        let mut handle = streamer.handle();

        let mut buf = vec![0; 2];
        let poll = handle
            .poll_read(&mut noop_context(), &mut buf)
            .map(|r| r.unwrap());

        assert_eq!(Poll::Ready(2), poll);
        assert_eq!(b"he", &buf[0..2]);

        let poll = handle
            .poll_read(&mut noop_context(), &mut buf)
            .map(|r| r.unwrap());

        assert_eq!(Poll::Ready(2), poll);
        assert_eq!(b"ll", &buf[0..2]);

        let poll = handle
            .poll_read(&mut noop_context(), &mut buf)
            .map(|r| r.unwrap());

        assert_eq!(Poll::Ready(1), poll);
        assert_eq!(b"o", &buf[0..1]);
    }

    #[test]
    fn large_read_buffer_small_chunk_size() {
        // Read from of a stream of many small chunks into a large buffer.
        //
        // Requires multiple polls to read the entire thing, even though we have
        // enough space in our buffer.

        let streamer = FixedSizedStreamer::new(b"hello", 2);
        let mut handle = streamer.handle();

        let mut buf = vec![0; 10];
        let poll = handle
            .poll_read(&mut noop_context(), &mut buf)
            .map(|r| r.unwrap());

        assert_eq!(Poll::Ready(2), poll);
        assert_eq!(b"he", &buf[0..2]);

        let poll = handle
            .poll_read(&mut noop_context(), &mut buf)
            .map(|r| r.unwrap());

        assert_eq!(Poll::Ready(2), poll);
        assert_eq!(b"ll", &buf[0..2]);

        let poll = handle
            .poll_read(&mut noop_context(), &mut buf)
            .map(|r| r.unwrap());

        assert_eq!(Poll::Ready(1), poll);
        assert_eq!(b"o", &buf[0..1]);
    }

    #[test]
    fn read_seek_read() {
        // Ensure we reset chunk read state when seeking.

        let streamer = FixedSizedStreamer::new(b"hello", 10);
        let mut handle = streamer.handle();

        let mut buf = vec![0; 10];
        let poll = handle
            .poll_read(&mut noop_context(), &mut buf)
            .map(|r| r.unwrap());

        assert_eq!(Poll::Ready(5), poll);
        assert_eq!(b"hello", &buf[0..5]);

        let poll = handle
            .poll_seek(&mut noop_context(), io::SeekFrom::Start(1))
            .map(|r| r.unwrap());

        assert_eq!(Poll::Ready(()), poll);

        let poll = handle
            .poll_read(&mut noop_context(), &mut buf)
            .map(|r| r.unwrap());

        assert_eq!(Poll::Ready(4), poll);
        assert_eq!(b"ello", &buf[0..4]);
    }
}
