use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::directory::{DirEntry, ReadDirHandle};
use glaredb_error::Result;
use reqwest::{Method, Request};
use url::Url;

use super::credentials::AwsCredentials;
use super::filesystem::S3RequestSigner;
use crate::client::HttpClient;
use crate::handle::RequestSigner;

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
                _ => unimplemented!(),
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
