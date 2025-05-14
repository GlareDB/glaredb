//! Shared list types/logic for object stores.
use std::fmt::{self, Debug};
use std::task::{Context, Poll};

use futures::{FutureExt, StreamExt};
use glaredb_core::runtime::filesystem::directory::DirEntry;
use glaredb_error::{DbError, Result};
use reqwest::{Request, StatusCode};

use crate::client::{HttpClient, HttpResponse};

pub trait List: Sync + Send + Debug {
    /// Create a request to use for the list.
    ///
    /// `continuation` should be added to the request to continue an in-progress
    /// paginate.
    fn create_request(&mut self, continuation: Option<String>) -> Result<Request>;

    /// Parse the response, writing the parsed entries to `entries`.
    ///
    /// Returns a continuation token if we need to keep paginating.
    fn parse_response(
        &mut self,
        response: &[u8],
        entries: &mut Vec<DirEntry>,
    ) -> Result<Option<String>>;
}

#[derive(Debug)]
pub struct ObjectStoreList<C: HttpClient, L: List> {
    client: C,
    list: L,
    state: ListRequestState<C>,
}

enum ListRequestState<C: HttpClient> {
    /// We're not currently doing a request.
    DoRequest {
        /// Optional continuation token from a previous request.
        ///
        /// Should be None if this is the first time we're making a request for
        /// this directory.
        continuation: Option<String>,
    },
    /// We're actively making the the request.
    Requesting { request_fut: C::RequestFuture },
    /// We are reading the body.
    Reading {
        /// Buffer to write the stream to.
        buf: Vec<u8>,
        /// The stream we're pulling from.
        stream: <C::Response as HttpResponse>::BytesStream,
    },
    /// We finished reading from the stream, and we didn't get a continuation
    /// token back. We want to emit nothing now.
    Exhausted,
}

impl<C> fmt::Debug for ListRequestState<C>
where
    C: HttpClient,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListRequestState").finish_non_exhaustive()
    }
}

impl<C, L> ObjectStoreList<C, L>
where
    C: HttpClient,
    L: List,
{
    pub fn new(client: C, list: L) -> Self {
        ObjectStoreList {
            client,
            list,
            state: ListRequestState::DoRequest { continuation: None },
        }
    }

    pub fn poll_list(&mut self, cx: &mut Context, ents: &mut Vec<DirEntry>) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                ListRequestState::DoRequest { continuation } => {
                    let request = self.list.create_request(continuation.take())?;
                    let request_fut = self.client.do_request(request);

                    self.state = ListRequestState::Requesting { request_fut };

                    // Continue... we're going to try to poll.
                }
                ListRequestState::Requesting { request_fut } => {
                    let resp = match request_fut.poll_unpin(cx) {
                        Poll::Ready(Ok(resp)) => resp,
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending, // Waker stored in future.
                    };

                    if resp.status() != StatusCode::OK {
                        // let text_res: Result<String> = block_on(async move {
                        //     let mut stream = resp.into_bytes_stream();
                        //     let mut buf = Vec::new();
                        //     while let Some(bs) = stream.try_next().await? {
                        //         buf.extend_from_slice(bs.as_ref());
                        //     }
                        //     Ok(String::from_utf8_lossy(&buf).to_string())
                        // });
                        // let text = text_res.unwrap_or(String::new());
                        // TODO: Probably want to read the body for the error
                        // message...
                        return Poll::Ready(Err(DbError::new("Failed to make list request")));
                    }

                    let stream = resp.into_bytes_stream();
                    self.state = ListRequestState::Reading {
                        buf: Vec::new(),
                        stream,
                    };

                    // Continue... we're going to try to read from the stream.
                }
                ListRequestState::Reading { buf, stream } => {
                    match stream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(bs))) => {
                            buf.extend_from_slice(bs.as_ref());
                            // Continue... we want to try to read more.
                        }
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                        Poll::Ready(None) => {
                            // Done, parse and emit.
                            let start_count = ents.len();
                            self.list.parse_response(buf.as_slice(), ents)?;

                            let appended = ents.len() - start_count;

                            // Ensure we only return if we emittied entries.
                            // TBD on when s3 would actually return zero items.
                            if appended > 0 {
                                return Poll::Ready(Ok(appended));
                            }

                            // Otherwise appended is zero... continue.
                        }
                        Poll::Pending => return Poll::Pending, // Waker stored in stream.
                    }
                }
                ListRequestState::Exhausted => {
                    // Done
                    return Poll::Ready(Ok(0));
                }
            }
        }
    }
}
