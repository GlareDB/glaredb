use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::directory::{DirEntry, ReadDirHandle};
use glaredb_error::Result;

use super::credentials::AwsCredentials;
use super::filesystem::S3RequestSigner;
use crate::client::HttpClient;

pub struct S3DirAccess {
    pub bucket: String,
    pub signer: S3RequestSigner,
}

#[derive(Debug)]
pub struct S3DirHandle<C: HttpClient> {
    client: C,
    /// The current prefix we're listing on.
    ///
    /// Should always include the trailing '/'.
    prefix: String,
}

#[derive(Debug)]
enum ListRequestState {
    /// We're not currently doing a request.
    DoRequest {
        /// Optional continuation token from a previous request.
        ///
        /// Should be None if this is the first time we're making a request for
        /// this directory.
        continuation: Option<String>,
    },
}

impl<C> ReadDirHandle for S3DirHandle<C>
where
    C: HttpClient,
{
    fn poll_list(&mut self, cx: &mut Context, ents: &mut Vec<DirEntry>) -> Poll<Result<usize>> {
        unimplemented!()
    }

    fn change_dir(&mut self, relative: impl Into<String>) -> Result<Self> {
        let mut new_prefix = format!("{}{}", self.prefix, relative.into());
        if !new_prefix.ends_with('/') {
            new_prefix.push('/');
        }

        unimplemented!()
    }
}
