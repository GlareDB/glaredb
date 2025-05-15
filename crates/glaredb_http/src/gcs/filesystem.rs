use std::io::SeekFrom;
use std::sync::Arc;
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::glob::{GlobSegments, is_glob};
use glaredb_core::runtime::filesystem::{
    FileHandle,
    FileOpenContext,
    FileStat,
    FileSystem,
    FileType,
    OpenFlags,
};
use glaredb_error::{DbError, OptionExt, Result, ResultExt, not_implemented};
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, HeaderValue};
use reqwest::{Method, Request, StatusCode};
use url::Url;

use super::credentials::{AccessToken, ServiceAccount};
use super::directory::GcsDirHandle;
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
}

#[derive(Debug)]
pub struct GcsLocation {
    pub(crate) bucket: String,
    pub(crate) object: String,
    pub(crate) endpoint: String,
}

impl GcsLocation {
    pub(crate) fn from_path(path: &str, _state: &GcsFileSystemState) -> Result<Self> {
        let url = Url::parse(path).context_fn(|| format!("Failed to parse '{path}' as a URL"))?;

        // Assumes gsutil format: 'gs://bucket/file.csv'
        let bucket = match url.host().required("Missing host on url")? {
            url::Host::Domain(host) => host,
            other => return Err(DbError::new(format!("Expected domain, got {other:?}"))),
        };
        let object = &url.path()[1..]; // Path includes a leading slash, slice it off.
        let endpoint = STORAGE_API_ENDPOINT;

        Ok(GcsLocation {
            bucket: bucket.to_string(),
            object: object.to_string(),
            endpoint: endpoint.to_string(),
        })
    }

    // <https://cloud.google.com/storage/docs/xml-api/overview>
    pub(crate) fn object_scoped_xml_api_url(&self) -> Result<Url> {
        let bucket = &self.bucket;
        let endpoint = &self.endpoint;
        let object = &self.object;

        // - bucket: The bucket containing the object (no leading '/')
        // - endpoint: The gcs endpoint to use.
        // - object: Path to the object, this should not include a leading '/'.
        let formatted = format!("https://{endpoint}/{bucket}/{object}");
        Url::parse(&formatted).context_fn(|| format!("Failed to parse '{formatted}' into url"))
    }

    // <https://cloud.google.com/storage/docs/json_api>
    pub(crate) fn bucket_scoped_json_api_url(&self) -> Result<Url> {
        let bucket = &self.bucket;
        let endpoint = &self.endpoint;
        let formatted = format!("https://{endpoint}/storage/v1/b/{bucket}/o");

        Url::parse(&formatted).context_fn(|| format!("Failed to parse '{formatted}' into url"))
    }
}

#[derive(Debug, Clone)]
pub struct GcsFileSystemState {
    token: Option<Arc<AccessToken>>,
}

impl<C> FileSystem for GcsFileSystem<C>
where
    C: HttpClient,
{
    const NAME: &str = "GCS";

    type FileHandle = GcsFileHandle<C>;
    type ReadDirHandle = GcsDirHandle<C>;
    type State = GcsFileSystemState;

    async fn load_state(&self, context: FileOpenContext<'_>) -> Result<Self::State> {
        let service_account = context.get_value("service_account")?;
        let token = match service_account {
            Some(s) => {
                let s = s.try_as_str()?;
                let service_account = ServiceAccount::try_from_str(s)?;
                // Fetch access token using service account.
                let token = service_account.fetch_access_token(&self.client).await?;
                Some(Arc::new(token))
            }
            None => None,
        };

        Ok(GcsFileSystemState { token })
    }

    async fn open(
        &self,
        flags: OpenFlags,
        path: &str,
        state: &Self::State,
    ) -> Result<Self::FileHandle> {
        if flags.is_write() {
            not_implemented!("write support for gcs filesystem")
        }
        if flags.is_create() {
            not_implemented!("create support for gcs filesystem")
        }

        let url = GcsLocation::from_path(path, state)?.object_scoped_xml_api_url()?;
        let mut request = Request::new(Method::HEAD, url.clone());
        // Authorize request.
        if let Some(tok) = &state.token {
            request = authorize_request(tok, request)?;
        }

        let resp = self.client.do_request(request).await?;
        let len = match resp.headers().get(CONTENT_LENGTH) {
            Some(v) => v
                .to_str()
                .context("Failed convert Content-Length header to string")?
                .parse::<usize>()
                .context("Failed to parse Content-Length")?,
            None => return Err(DbError::new("Missing Content-Length header for file")),
        };

        let signer = GcsRequestSigner {
            token: state.token.clone(),
        };
        let handle = HttpFileHandle::new(url, len, self.client.clone(), signer);

        Ok(GcsFileHandle { handle })
    }

    async fn stat(&self, path: &str, state: &Self::State) -> Result<Option<FileStat>> {
        let url = GcsLocation::from_path(path, state)?.object_scoped_xml_api_url()?;

        let mut request = Request::new(Method::HEAD, url.clone());
        if let Some(tok) = &state.token {
            request = authorize_request(tok, request)?;
        }

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

    fn read_dir(&self, dir: &str, state: &Self::State) -> Result<Self::ReadDirHandle> {
        let location = GcsLocation::from_path(dir, state)?;
        let signer = GcsRequestSigner {
            token: state.token.clone(),
        };

        GcsDirHandle::try_new(self.client.clone(), location, signer)
    }

    fn glob_segments(glob: &str) -> Result<GlobSegments> {
        // TODO: Duplicated with s3
        let trimmed = match glob.strip_prefix("gs://") {
            Some(trimmed) => trimmed,
            None => return Err(DbError::new(format!("Glob missing 'gs://' scheme: {glob}"))),
        };

        // Now we parse the segments from the trimmed string
        //
        // First segment is the bucket.
        let mut segments = trimmed.split('/').filter(|s| !s.is_empty());
        let bucket = match segments.next() {
            Some(bucket) => {
                if is_glob(bucket) {
                    return Err(DbError::new("Cannot have a glob in the bucket name"));
                }
                bucket
            }
            None => return Err(DbError::new("Cannot create glob from no segments")),
        };

        let mut segments: Vec<_> = segments.collect();
        if segments.is_empty() {
            return Err(DbError::new(
                "Cannot have zero segments after parsing bucket",
            ));
        }

        // Find the root dir relative to the bucket to use.
        let mut root_dir_rel = Vec::new();
        while !segments.is_empty() && !is_glob(segments[0]) {
            root_dir_rel.push(segments.remove(0));
        }

        let root_dir_rel = root_dir_rel.join("/");
        // Now put it back into the 'gs://...' format.
        let root_dir = format!("gs://{bucket}/{root_dir_rel}");

        let segments = segments.into_iter().map(|s| s.to_string()).collect();

        Ok(GlobSegments { root_dir, segments })
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
    token: Option<Arc<AccessToken>>,
}

impl RequestSigner for GcsRequestSigner {
    fn sign(&self, request: Request) -> Result<Request> {
        match self.token.as_ref() {
            Some(tok) => authorize_request(tok, request),
            None => Ok(request),
        }
    }
}

#[derive(Debug)]
pub struct GcsFileHandle<C: HttpClient> {
    handle: HttpFileHandle<C, GcsRequestSigner>,
}

impl<C> FileHandle for GcsFileHandle<C>
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
            "GcsFileHandle does not yet support writing",
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

fn authorize_request(token: &AccessToken, mut request: Request) -> Result<Request> {
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", token.access_token))
            .context("Failed to set AUTHORIZATION header value")?,
    );

    Ok(request)
}
