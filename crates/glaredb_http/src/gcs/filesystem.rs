use std::io::SeekFrom;
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::{
    File,
    FileOpenContext,
    FileStat,
    FileSystem,
    FileType,
    OpenFlags,
};
use glaredb_error::{DbError, OptionExt, Result, ResultExt, not_implemented};
use reqwest::header::CONTENT_LENGTH;
use reqwest::{Method, Request, StatusCode};
use url::Url;

use crate::client::{HttpClient, HttpResponse};
use crate::handle::{HttpFileHandle, RequestSigner};

pub const STORAGE_API_ENDPOINT: &str = "storage.googleapis.com";

#[derive(Debug)]
pub struct GcsFileSystem<C: HttpClient> {
    client: C,
}

impl<C> GcsFileSystem<C>
where
    C: HttpClient,
{
    pub fn new(client: C) -> Self {
        GcsFileSystem { client }
    }

    fn gcs_url_from_path(&self, path: &str) -> Result<Url> {
        let url = Url::parse(path).context_fn(|| format!("Failed to parse '{path}' as a URL"))?;

        // Assumes gsutil format: 'gs://bucket/file.csv'
        let bucket = match url.host().required("Missing host on url")? {
            url::Host::Domain(host) => host,
            other => return Err(DbError::new(format!("Expected domain, got {other:?}"))),
        };
        let object = url.path(); // Should include leading '/';
        let endpoint = STORAGE_API_ENDPOINT;

        // - bucket: The bucket containing the object (no leading '/')
        // - endpoint: The gcs endpoint to use.
        // - object: Path to the object, this should include a leading '/'.
        let formatted = format!("https://{endpoint}/{bucket}{object}");
        Url::parse(&formatted).context_fn(|| format!("Failed to parse '{formatted}' into url"))
    }
}

#[derive(Debug, Clone)]
pub struct GcsFileSystemState {
    // TODO: Creds
}

impl<C> FileSystem for GcsFileSystem<C>
where
    C: HttpClient,
{
    type File = GcsFileHandle<C>;
    type State = GcsFileSystemState;

    fn state_from_context(&self, _context: FileOpenContext) -> Result<Self::State> {
        // TODO: Creds
        Ok(GcsFileSystemState {})
    }

    async fn open(&self, flags: OpenFlags, path: &str, _state: &Self::State) -> Result<Self::File> {
        if flags.is_write() {
            not_implemented!("write support for gcs filesystem")
        }
        if flags.is_create() {
            not_implemented!("create support for gcs filesystem")
        }

        let url = self.gcs_url_from_path(path)?;
        let request = Request::new(Method::HEAD, url.clone());
        // TODO: Creds

        let resp = self.client.do_request(request).await?;
        let len = match resp.headers().get(CONTENT_LENGTH) {
            Some(v) => v
                .to_str()
                .context("Failed convert Content-Length header to string")?
                .parse::<usize>()
                .context("Failed to parse Content-Length")?,
            None => return Err(DbError::new("Missing Content-Length header for file")),
        };

        let signer = GcsRequestSigner {};
        let handle = HttpFileHandle::new(url, len, self.client.clone(), signer);

        Ok(GcsFileHandle { handle })
    }

    async fn stat(&self, path: &str, _state: &Self::State) -> Result<Option<FileStat>> {
        let url = self.gcs_url_from_path(path)?;

        let request = Request::new(Method::HEAD, url.clone());
        // TODO: Creds
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

    fn can_handle_path(&self, path: &str) -> bool {
        match Url::parse(path) {
            Ok(url) => {
                let scheme = url.scheme();
                scheme == "gs"
            }
            Err(_) => false,
        }
    }
}

#[derive(Debug)]
pub struct GcsRequestSigner {
    // TODO: Creds
}

impl RequestSigner for GcsRequestSigner {
    fn sign(&self, request: Request) -> Result<Request> {
        // TODO: Sign if creds
        Ok(request)
    }
}

#[derive(Debug)]
pub struct GcsFileHandle<C: HttpClient> {
    handle: HttpFileHandle<C, GcsRequestSigner>,
}

impl<C> File for GcsFileHandle<C>
where
    C: HttpClient,
{
    fn path(&self) -> &str {
        self.handle.url.as_str()
    }

    fn size(&self) -> usize {
        self.handle.len
    }

    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        self.handle.poll_read(cx, buf)
    }

    fn poll_write(&mut self, _cx: &mut Context, _buf: &[u8]) -> Poll<Result<usize>> {
        Poll::Ready(Err(DbError::new(
            "GcsFileHandle does not yest support writing",
        )))
    }

    fn poll_seek(&mut self, cx: &mut Context, seek: SeekFrom) -> Poll<Result<()>> {
        self.handle.poll_seek(cx, seek)
    }

    fn poll_flush(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Err(DbError::new(
            "GcsFileHandle does not yet support flushing",
        )))
    }
}
