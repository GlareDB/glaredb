use std::io::SeekFrom;
use std::task::{Context, Poll};

use chrono::Utc;
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
use reqwest::header::CONTENT_LENGTH;
use reqwest::{Method, Request, StatusCode};
use url::Url;

use super::credentials::{AwsCredentials, AwsRequestAuthorizer};
use super::directory::S3DirHandle;
use crate::client::{HttpClient, HttpResponse};
use crate::handle::{HttpFileHandle, RequestSigner};

pub const AWS_ENDPOINT: &str = "amazonaws.com";

#[derive(Debug)]
pub struct S3FileSystem<C: HttpClient> {
    default_region: &'static str,
    client: C,
}

impl<C> S3FileSystem<C>
where
    C: HttpClient,
{
    pub fn new(client: C, default_region: &'static str) -> Self {
        S3FileSystem {
            default_region,
            client,
        }
    }
}

#[derive(Debug)]
pub struct S3Location {
    pub(crate) bucket: String,
    pub(crate) region: String,
    pub(crate) object: String,
    pub(crate) endpoint: String,
}

impl S3Location {
    pub(crate) fn from_path(path: &str, state: &S3FileSystemState) -> Result<Self> {
        let url = Url::parse(path).context_fn(|| format!("Failed to parse '{path}' as a URL"))?;

        // Assumes s3 format: 's3://bucket/file.csv'
        let bucket = match url.host().required("Missing host on url")? {
            url::Host::Domain(host) => host,
            other => return Err(DbError::new(format!("Expected domain, got {other:?}"))),
        };
        let object = &url.path()[1..]; // Path includes a leading slash, slice it off.
        let region = &state.region;
        let endpoint = AWS_ENDPOINT;

        Ok(S3Location {
            bucket: bucket.to_string(),
            region: region.to_string(),
            object: object.to_string(),
            endpoint: endpoint.to_string(),
        })
    }

    pub(crate) fn url(&self) -> Result<Url> {
        let bucket = &self.bucket;
        let region = &self.region;
        let endpoint = &self.endpoint;
        let object = &self.object;

        // - bucket: The bucket containing the object.
        // - region: Region containing the bucket.
        // - endpoint: The s3 endpoint to use.
        // - object: Path to the object, this should not include a leading '/'.
        let formatted = format!("https://{bucket}.s3.{region}.{endpoint}/{object}");
        Url::parse(&formatted).context_fn(|| format!("Failed to parse '{formatted}' into url"))
    }
}

#[derive(Debug, Clone)]
pub struct S3FileSystemState {
    region: String,
    creds: Option<AwsCredentials>,
}

impl<C> FileSystem for S3FileSystem<C>
where
    C: HttpClient,
{
    const NAME: &str = "S3";

    type FileHandle = S3FileHandle<C>;
    type ReadDirHandle = S3DirHandle<C>;
    type State = S3FileSystemState;

    fn state_from_context(&self, context: FileOpenContext) -> Result<Self::State> {
        let key_id = context.get_value("access_key_id")?;
        let secret = context.get_value("secret_access_key")?;
        let region = context
            .get_value("region")?
            .unwrap_or(self.default_region.into())
            .try_into_string()?;

        let creds = match (key_id, secret) {
            (Some(key_id), Some(secret)) => Some(AwsCredentials {
                key_id: key_id.try_into_string()?,
                secret: secret.try_into_string()?,
            }),
            (None, None) => None,
            (Some(_), None) => return Err(DbError::new("Missing 'secret_access_key' argument")),
            (None, Some(_)) => return Err(DbError::new("Missing 'access_key_id' argument")),
        };

        Ok(S3FileSystemState { creds, region })
    }

    async fn open(
        &self,
        flags: OpenFlags,
        path: &str,
        state: &Self::State,
    ) -> Result<Self::FileHandle> {
        if flags.is_write() {
            not_implemented!("write support for s3 filesystem")
        }
        if flags.is_create() {
            not_implemented!("create support for s3 filesystem")
        }
        let location = S3Location::from_path(path, state)?.url()?;

        let mut request = Request::new(Method::HEAD, location.clone());
        // If we don't have creds, we can skip signing.
        if let Some(creds) = &state.creds {
            request = authorize_request(creds, &state.region, request)?;
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

        let signer = S3RequestSigner {
            region: state.region.clone(),
            creds: state.creds.clone(),
        };
        let handle = HttpFileHandle::new(location, len, self.client.clone(), signer);

        Ok(S3FileHandle { handle })
    }

    async fn stat(&self, path: &str, state: &Self::State) -> Result<Option<FileStat>> {
        let location = S3Location::from_path(path, state)?.url()?;

        let mut request = Request::new(Method::HEAD, location.clone());
        if let Some(creds) = &state.creds {
            request = authorize_request(creds, &state.region, request)?;
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
        let location = S3Location::from_path(dir, state)?;
        let signer = S3RequestSigner {
            region: state.region.clone(),
            creds: state.creds.clone(),
        };

        let dir = S3DirHandle::try_new(self.client.clone(), location, signer)?;

        Ok(dir)
    }

    fn glob_segments(glob: &str) -> Result<GlobSegments> {
        let trimmed = match glob.strip_prefix("s3://") {
            Some(trimmed) => trimmed,
            None => return Err(DbError::new("Glob missing 's3://' scheme: {glob}")),
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
        // Now put it back into the 's3://...' format.
        let root_dir = format!("s3://{bucket}/{root_dir_rel}");

        let segments = segments.into_iter().map(|s| s.to_string()).collect();

        Ok(GlobSegments { root_dir, segments })
    }

    fn can_handle_path(&self, path: &str) -> bool {
        match Url::parse(path) {
            Ok(url) => {
                let scheme = url.scheme();
                scheme == "s3"
            }
            Err(_) => false,
        }
    }
}

#[derive(Debug)]
pub struct S3RequestSigner {
    region: String,
    creds: Option<AwsCredentials>,
}

impl RequestSigner for S3RequestSigner {
    fn sign(&self, request: Request) -> Result<Request> {
        match &self.creds {
            Some(creds) => authorize_request(creds, &self.region, request),
            None => {
                // Anonymous access, no need to sign/authorize the request.
                Ok(request)
            }
        }
    }
}

#[derive(Debug)]
pub struct S3FileHandle<C: HttpClient> {
    handle: HttpFileHandle<C, S3RequestSigner>,
}

impl<C> FileHandle for S3FileHandle<C>
where
    C: HttpClient,
{
    fn path(&self) -> &str {
        // This returns the https url, not the s3 one the user provided. We
        // might want to store the original too for informational purposes.
        self.handle.url.as_str()
    }

    fn size(&self) -> usize {
        self.handle.len
    }

    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        self.handle.poll_read(cx, buf)
    }

    fn poll_write(&mut self, _cx: &mut Context, _buf: &[u8]) -> Poll<Result<usize>> {
        // yet
        Poll::Ready(Err(DbError::new(
            "S3FileHandle does not yet support writing",
        )))
    }

    fn poll_seek(&mut self, cx: &mut Context, seek: SeekFrom) -> Poll<Result<()>> {
        self.handle.poll_seek(cx, seek)
    }

    fn poll_flush(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        // yet
        Poll::Ready(Err(DbError::new(
            "S3FileHandle does not yet support flushing",
        )))
    }
}

fn authorize_request(creds: &AwsCredentials, region: &str, request: Request) -> Result<Request> {
    let authorizer = AwsRequestAuthorizer {
        date: Utc::now(),
        credentials: creds,
        region,
    };
    authorizer.authorize(request)
}
