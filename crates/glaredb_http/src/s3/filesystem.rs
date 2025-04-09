use std::io::SeekFrom;
use std::task::{Context, Poll};

use chrono::Utc;
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

use super::credentials::{AwsCredentials, AwsRequestAuthorizer};
use crate::client::{HttpClient, HttpResponse};
use crate::handle::{ChunkReadState, HttpFileHandle, RequestSigner};

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

    /// Construct a url pointing to the s3 resource.
    fn s3_location_from_path(&self, path: &str) -> Result<Url> {
        let url = Url::parse(path).context_fn(|| format!("Failed to parse '{path}' as a URL"))?;

        // Assumes s3 format: 's3://bucket/file.csv'
        let bucket = match url.host().required("Missing host on url")? {
            url::Host::Domain(host) => host,
            other => return Err(DbError::new(format!("Expected domain, got {other:?}"))),
        };
        let object = url.path(); // Should include leading '/';
        let region = self.default_region;
        let endpoint = AWS_ENDPOINT;

        // - bucket: The bucket containing the object.
        // - region: Region containing the bucket.
        // - endpoint: The s3 endpoint to use.
        // - object: Path to the object, this should include a leading '/'.
        let formatted = format!("https://{bucket}.s3.{region}.{endpoint}{object}");
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
    type File = S3FileHandle<C>;
    type State = S3FileSystemState;

    fn state_from_context(&self, context: FileOpenContext) -> Result<Self::State> {
        let key_id = context.get_value("access_key_id")?;
        let secret = context.get_value("secret_access_key")?;

        let creds = match (key_id, secret) {
            (Some(key_id), Some(secret)) => Some(AwsCredentials {
                key_id: key_id.try_into_string()?,
                secret: secret.try_into_string()?,
            }),
            (None, None) => None,
            (Some(_), None) => return Err(DbError::new("Missing 'secret_access_key' argument")),
            (None, Some(_)) => return Err(DbError::new("Missing 'access_key_id' argument")),
        };

        // TODO: Get region from context.

        Ok(S3FileSystemState {
            creds,
            region: self.default_region.to_string(),
        })
    }

    // TODO: Need a way to pass in region.
    async fn open(&self, flags: OpenFlags, path: &str, state: &Self::State) -> Result<Self::File> {
        if flags.is_write() {
            not_implemented!("write support for s3 filesystem")
        }
        if flags.is_create() {
            not_implemented!("create support for s3 filesystem")
        }
        let location = self.s3_location_from_path(path)?;

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

        Ok(S3FileHandle {
            handle: HttpFileHandle {
                url: location,
                pos: 0,
                chunk: ChunkReadState::None,
                len,
                client: self.client.clone(),
                signer: S3RequestSigner {
                    region: state.region.clone(),
                    creds: state.creds.clone(),
                },
            },
        })
    }

    async fn stat(&self, path: &str, state: &Self::State) -> Result<Option<FileStat>> {
        let location = self.s3_location_from_path(path)?;

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

impl<C> File for S3FileHandle<C>
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
        Poll::Ready(Err(DbError::new("S3FileHandle does not support writing")))
    }

    fn poll_seek(&mut self, cx: &mut Context, seek: SeekFrom) -> Poll<Result<()>> {
        self.handle.poll_seek(cx, seek)
    }

    fn poll_flush(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        // yet
        Poll::Ready(Err(DbError::new("S3FileHandle does not support flushing")))
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
