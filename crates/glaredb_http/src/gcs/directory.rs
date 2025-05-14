use std::sync::Arc;
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::directory::{DirEntry, ReadDirHandle};
use glaredb_error::{Result, ResultExt};
use reqwest::{Method, Request};
use url::Url;

use super::filesystem::{GcsLocation, GcsRequestSigner};
use crate::client::HttpClient;
use crate::gcs::list::GcsListResponse;
use crate::handle::RequestSigner;
use crate::list::{List, ObjectStoreList};

#[derive(Debug)]
pub struct GcsDirAccess {
    /// JSON api url.
    ///
    /// 'https://storage.googleapis.com/storage/v1/b/bucket/o'
    url: Url,
    bucket: String,
    signer: GcsRequestSigner,
}

#[derive(Debug)]
pub struct GcsList {
    access: Arc<GcsDirAccess>,
    /// The current prefix we're listing on.
    ///
    /// Should include the trailing '/' if the prefix isn't empty.
    prefix: String,
}

impl List for GcsList {
    fn create_request(&mut self, continuation: Option<String>) -> Result<Request> {
        let mut url = self.access.url.clone();
        url.query_pairs_mut()
            .append_pair("prefix", &self.prefix)
            .append_pair("delimiter", "/");

        if let Some(continuation) = continuation {
            url.query_pairs_mut()
                .append_pair("pageToken", &continuation);
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
        let list_resp: GcsListResponse =
            serde_json::from_slice(response).context("Failed to deserialize list response")?;

        // "items" are objects, treat them as files.
        entries.extend(
            list_resp.items.into_iter().map(|item| {
                DirEntry::new_file(format!("gs://{}/{}", self.access.bucket, item.name))
            }),
        );

        // "prefixes" are common prefixes, treat them as subdirs.
        entries.extend(
            list_resp
                .prefixes
                .into_iter()
                .map(|prefix| DirEntry::new_dir(prefix)),
        );

        Ok(list_resp.next_page_token)
    }
}

#[derive(Debug)]
pub struct GcsDirHandle<C: HttpClient> {
    list: ObjectStoreList<C, GcsList>,
}

impl<C> GcsDirHandle<C>
where
    C: HttpClient,
{
    pub fn try_new(client: C, location: GcsLocation, signer: GcsRequestSigner) -> Result<Self> {
        let url = location.bucket_scoped_json_api_url()?;

        // Use object as prefix.
        let mut prefix = location.object;
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }

        let list = GcsList {
            access: Arc::new(GcsDirAccess {
                url,
                bucket: location.bucket.clone(), // TODO: What if just put location here?
                signer,
            }),
            prefix,
        };

        Ok(GcsDirHandle {
            list: ObjectStoreList::new(client, list),
        })
    }
}

impl<C> ReadDirHandle for GcsDirHandle<C>
where
    C: HttpClient,
{
    fn poll_list(&mut self, cx: &mut Context, ents: &mut Vec<DirEntry>) -> Poll<Result<usize>> {
        self.list.poll_list(cx, ents)
    }

    fn change_dir(&mut self, relative: impl Into<String>) -> Result<Self> {
        // TODO: Need to test.

        let relative = relative.into();
        let mut new_prefix = format!("{}{}", self.list.list.prefix, relative);
        if !new_prefix.ends_with('/') {
            new_prefix.push('/');
        }

        let list = ObjectStoreList::new(
            self.list.client.clone(),
            GcsList {
                access: self.list.list.access.clone(),
                prefix: new_prefix,
            },
        );

        Ok(GcsDirHandle { list })
    }
}
