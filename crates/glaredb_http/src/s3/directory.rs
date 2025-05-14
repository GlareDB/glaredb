use std::fmt;
use std::io::Cursor;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{FutureExt, StreamExt};
use glaredb_core::runtime::filesystem::directory::{DirEntry, ReadDirHandle};
use glaredb_error::{DbError, Result, ResultExt};
use reqwest::{Method, Request, StatusCode};
use url::Url;

use super::filesystem::{S3Location, S3RequestSigner};
use crate::client::{HttpClient, HttpResponse};
use crate::handle::RequestSigner;
use crate::list::List;
use crate::s3::list::S3ListResponse;

#[derive(Debug)]
pub struct S3List {
    access: Arc<S3DirAccess>,
    /// The current prefix we're listing on.
    ///
    /// Should include the trailing '/' if the prefix isn't empty.
    prefix: String,
}

impl List for S3List {
    fn create_request(&mut self, continuation: Option<String>) -> Result<Request> {
        let mut url = self.access.url.clone();
        url.query_pairs_mut()
            .append_pair("list-type", "2")
            .append_pair("prefix", &self.prefix)
            .append_pair("delimiter", "/");

        if let Some(continuation) = continuation {
            url.query_pairs_mut()
                .append_pair("continuation-token", &continuation);
        }

        let request = Request::new(Method::GET, url);
        let request = self.access.signer.sign(request)?;

        Ok(request)
    }

    fn parse_response(
        &mut self,
        response: &[u8],
        entries: &mut Vec<DirEntry>,
    ) -> Result<Option<String>> {
        let list_resp: S3ListResponse = quick_xml::de::from_reader(Cursor::new(response))
            .context("Failed to deserialize list response")?;

        // For all content objects, treat them as files.
        if let Some(contents) = list_resp.contents {
            entries.extend(contents.into_iter().map(|content| {
                DirEntry::new_file(format!("s3://{}/{}", self.access.bucket, content.key))
            }))
        }

        // Now treat common prefixes as subdirs.
        if let Some(common_prefixes) = list_resp.common_prefixes {
            entries.extend(common_prefixes.into_iter().map(|prefix| {
                // No need to prepend, the full prefix is
                // returned.
                DirEntry::new_dir(prefix.prefix)
            }));
        }

        Ok(list_resp.next_continuation_token)
    }
}

#[derive(Debug)]
pub struct S3DirAccess {
    /// URL for the bucket.
    ///
    /// 'https://bucket.s3.region.amazonaws.com'
    url: Url,
    bucket: String,
    signer: S3RequestSigner,
}

#[derive(Debug)]
pub struct S3DirHandle<C: HttpClient> {
    client: C,
    access: Arc<S3DirAccess>,
    /// The current prefix we're listing on.
    ///
    /// Should include the trailing '/' if the prefix isn't empty.
    prefix: String,
    /// Current state of the handle.
    state: ListRequestState<C>,
}

impl<C> S3DirHandle<C>
where
    C: HttpClient,
{
    pub fn try_new(client: C, mut location: S3Location, signer: S3RequestSigner) -> Result<Self> {
        // We only care about the bucket root when generating the url. So take
        // the existing object to use as the initial prefix, and replace with
        // empty one.
        let mut prefix = std::mem::take(&mut location.object);

        // 'https://bucket.s3.region.amazonaws.com'
        let url = location.url()?;

        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }

        Ok(S3DirHandle {
            client,
            access: Arc::new(S3DirAccess {
                url,
                bucket: location.bucket,
                signer,
            }),
            prefix,
            state: ListRequestState::DoRequest { continuation: None },
        })
    }
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
                            let list_resp: S3ListResponse =
                                quick_xml::de::from_reader(Cursor::new(buf))
                                    .context("Failed to deserialize list response")?;

                            let start_len = ents.len();

                            // For all content objects, treat them as files.
                            if let Some(contents) = list_resp.contents {
                                ents.extend(contents.into_iter().map(|content| {
                                    DirEntry::new_file(format!(
                                        "s3://{}/{}",
                                        self.access.bucket, content.key
                                    ))
                                }))
                            }

                            // Now treat common prefixes as subdirs.
                            if let Some(common_prefixes) = list_resp.common_prefixes {
                                ents.extend(common_prefixes.into_iter().map(|prefix| {
                                    // No need to prepend, the full prefix is
                                    // returned.
                                    DirEntry::new_dir(prefix.prefix)
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
        let relative = relative.into();
        let mut new_prefix = format!("{}{}", self.prefix, relative);
        if !new_prefix.ends_with('/') {
            new_prefix.push('/');
        }

        Ok(S3DirHandle {
            client: self.client.clone(),
            access: self.access.clone(),
            prefix: new_prefix,
            state: ListRequestState::DoRequest { continuation: None },
        })
    }
}
