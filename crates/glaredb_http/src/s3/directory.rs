use std::fmt;
use std::io::Cursor;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{FutureExt, StreamExt};
use glaredb_core::runtime::filesystem::FileType;
use glaredb_core::runtime::filesystem::directory::{DirEntry, ReadDirHandle};
use glaredb_error::{DbError, Result, ResultExt};
use reqwest::{Method, Request, StatusCode};
use url::Url;

use super::filesystem::S3RequestSigner;
use crate::client::{HttpClient, HttpResponse};
use crate::handle::RequestSigner;
use crate::s3::list::S3ListResponse;

#[derive(Debug)]
pub struct S3DirAccess {
    /// URL for the bucket.
    ///
    /// 'https://bucket.s3.region.amazonaws.com'
    pub url: Url,
    pub signer: S3RequestSigner,
}

#[derive(Debug)]
pub struct S3DirHandle<C: HttpClient> {
    client: C,
    access: Arc<S3DirAccess>,
    /// The current prefix we're listing on.
    ///
    /// Should always include the trailing '/'.
    prefix: String,
    /// Current state of the handle.
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

impl<C> ReadDirHandle for S3DirHandle<C>
where
    C: HttpClient,
{
    fn poll_list(&mut self, cx: &mut Context, ents: &mut Vec<DirEntry>) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                ListRequestState::DoRequest { continuation } => {
                    let mut url = self.access.url.clone();
                    url.query_pairs_mut()
                        .append_pair("list-type", "2")
                        .append_pair("prefix", &self.prefix)
                        .append_pair("delimiter", "/");

                    if let Some(continuation) = continuation.take() {
                        url.query_pairs_mut()
                            .append_pair("continuation-token", &continuation);
                    }

                    let request = Request::new(Method::GET, url);
                    let request = self.access.signer.sign(request)?;

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
                            let list_resp: S3ListResponse =
                                quick_xml::de::from_reader(Cursor::new(buf))
                                    .context("Failed to deserialize list response")?;

                            let start_len = ents.len();

                            // For all content objects, treat them as files.
                            //
                            // Content object keys are relative to the prefix we
                            // supplied when we made the request.
                            ents.extend(list_resp.contents.into_iter().map(|content| DirEntry {
                                path: format!("{}{}", self.prefix, content.key),
                                file_type: FileType::File,
                            }));

                            // Now treat common prefixes as subdirs.
                            if let Some(common_prefixes) = list_resp.common_prefixes {
                                ents.extend(common_prefixes.into_iter().map(|prefix| {
                                    DirEntry {
                                        path: prefix.prefix, // No need to prepend, the full prefix is returned.
                                        file_type: FileType::Directory,
                                    }
                                }));
                            }

                            let appended = ents.len() - start_len;

                            if list_resp.next_continuation_token.is_some() {
                                // If we have a contination token, need to init a
                                // new request on the next poll.
                                self.state = ListRequestState::DoRequest {
                                    continuation: list_resp.next_continuation_token,
                                };
                            } else {
                                // Otherwise we have nothing else to do (at this dir level).
                                self.state = ListRequestState::Exhausted;
                            }

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

    fn change_dir(&mut self, relative: impl Into<String>) -> Result<Self> {
        let mut new_prefix = format!("{}{}", self.prefix, relative.into());
        if !new_prefix.ends_with('/') {
            new_prefix.push('/');
        }

        unimplemented!()
    }
}
