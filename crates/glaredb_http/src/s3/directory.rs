use std::sync::Arc;
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::directory::{DirEntry, ReadDirHandle};
use glaredb_error::{Result, ResultExt};
use reqwest::{Method, Request};
use url::Url;

use super::filesystem::{S3Location, S3RequestSigner};
use crate::client::HttpClient;
use crate::handle::RequestSigner;
use crate::list::{List, ObjectStoreList};
use crate::s3::list::S3ListResponse;

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
        let response = std::str::from_utf8(response).context("List response not valid utf8")?;
        let list_resp: S3ListResponse =
            quick_xml::de::from_str(response).context("Failed to deserialize list response")?;

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

        Ok(list_resp.next_continuation_token.map(|s| s.to_string()))
    }
}

#[derive(Debug)]
pub struct S3DirHandle<C: HttpClient> {
    list: ObjectStoreList<C, S3List>,
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

        let list = S3List {
            access: Arc::new(S3DirAccess {
                url,
                bucket: location.bucket,
                signer,
            }),
            prefix,
        };

        Ok(S3DirHandle {
            list: ObjectStoreList::new(client, list),
        })
    }
}

impl<C> ReadDirHandle for S3DirHandle<C>
where
    C: HttpClient,
{
    fn poll_list(&mut self, cx: &mut Context, ents: &mut Vec<DirEntry>) -> Poll<Result<usize>> {
        self.list.poll_list(cx, ents)
    }

    fn change_dir(&mut self, relative: impl Into<String>) -> Result<Self> {
        let relative = relative.into();
        let mut new_prefix = format!("{}{}", self.list.list.prefix, relative);
        if !new_prefix.ends_with('/') {
            new_prefix.push('/');
        }

        let list = ObjectStoreList::new(
            self.list.client.clone(),
            S3List {
                access: self.list.list.access.clone(),
                prefix: new_prefix,
            },
        );

        Ok(S3DirHandle { list })
    }
}
