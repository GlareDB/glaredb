use glaredb_core::runtime::filesystem::file_list::NotImplementedDirList;
use glaredb_core::runtime::filesystem::{
    FileOpenContext,
    FileStat,
    FileSystem,
    FileType,
    OpenFlags,
};
use glaredb_error::{DbError, Result, ResultExt, not_implemented};
use reqwest::header::CONTENT_LENGTH;
use reqwest::{Method, Request, StatusCode};
use url::Url;

use crate::client::{HttpClient, HttpResponse};
use crate::handle::{HttpFileHandle, RequestSigner};

#[derive(Debug)]
pub struct HttpFileSystem<C: HttpClient> {
    client: C,
}

#[derive(Debug, Clone, Copy)]
pub struct NopRequestSigner;

impl RequestSigner for NopRequestSigner {
    fn sign(&self, request: Request) -> Result<Request> {
        Ok(request)
    }
}

impl<C> HttpFileSystem<C>
where
    C: HttpClient,
{
    pub fn new(client: C) -> Self {
        HttpFileSystem { client }
    }
}

impl<C> FileSystem for HttpFileSystem<C>
where
    C: HttpClient,
{
    type File = HttpFileHandle<C, NopRequestSigner>;
    type State = ();
    type DirList = NotImplementedDirList;

    fn state_from_context(&self, _context: FileOpenContext) -> Result<Self::State> {
        Ok(())
    }

    async fn open(&self, flags: OpenFlags, path: &str, _state: &()) -> Result<Self::File> {
        if flags.is_write() {
            not_implemented!("write support for http filesystem")
        }
        if flags.is_create() {
            not_implemented!("create support for http filesystem")
        }

        let url = Url::parse(path).context_fn(|| format!("Failed to parse '{path}' as a URL"))?;
        let request = Request::new(Method::HEAD, url.clone());
        let resp = self.client.do_request(request).await?;

        // TODO: If we can't get content length, we can optionally just download
        // the whole file and buffer it.
        let len = match resp.headers().get(CONTENT_LENGTH) {
            Some(v) => v
                .to_str()
                .context("Failed convert Content-Length header to string")?
                .parse::<usize>()
                .context("Failed to parse Content-Length")?,
            None => return Err(DbError::new("Missing Content-Length header for file")),
        };

        let handle = HttpFileHandle::new(url, len, self.client.clone(), NopRequestSigner);

        Ok(handle)
    }

    async fn stat(&self, path: &str, _state: &()) -> Result<Option<FileStat>> {
        let url = Url::parse(path).context("Failed to parse http filesystem path as a URL")?;
        let request = Request::new(Method::HEAD, url.clone());
        let resp = self.client.do_request(request).await?;

        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if status.is_success() {
            return Ok(Some(FileStat {
                file_type: FileType::File,
            }));
        }

        Err(DbError::new(format!("Unexpected status code: {status}")))
    }

    fn read_dir(&self, _prefix: &str, _state: &Self::State) -> Self::DirList {
        NotImplementedDirList
    }

    fn can_handle_path(&self, path: &str) -> bool {
        match Url::parse(path) {
            Ok(url) => {
                let scheme = url.scheme();
                scheme == "http" || scheme == "https"
            }
            Err(_) => false,
        }
    }
}
