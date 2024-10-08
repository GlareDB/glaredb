pub mod credentials;
pub mod list;

use bytes::{Buf, Bytes};
use chrono::Utc;
use credentials::{AwsCredentials, AwsRequestAuthorizer};
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt, TryStreamExt};
use list::{S3ListContents, S3ListResponse};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use reqwest::header::{CONTENT_LENGTH, RANGE};
use reqwest::{Method, Request, StatusCode};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::http::{format_range_header, read_text, HttpClient, HttpResponse};
use crate::FileSource;

// TODO: Lots of cloning...

const AWS_ENDPOINT: &str = "amazonaws.com";

/// A location to a single object in S3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Location {
    pub url: Url,
}

impl S3Location {
    pub fn is_s3_location(url: &Url) -> bool {
        // Very sophisticated.
        // TODO: Extend to support https schemas with aws endpoint.
        url.scheme() == "s3"
    }

    pub fn from_url(url: Url, region: &str) -> Result<S3Location> {
        match url.scheme() {
            "s3" => {
                let bucket = match url.host() {
                    Some(url::Host::Domain(host)) => host,
                    Some(_) => return Err(RayexecError::new("Unexpected host")),
                    None => return Err(RayexecError::new("Missing host")),
                };

                let object = url.path(); // Should include leading slash.
                let formatted = format!("https://{bucket}.s3.{region}.{AWS_ENDPOINT}{object}");
                let url = Url::parse(&formatted)
                    .context_fn(|| format!("Failed to parse '{formatted}' into url"))?;

                Ok(S3Location { url })
            }
            "https" => {
                not_implemented!("non-vanity s3 urls")
            }
            scheme => Err(RayexecError::new(format!(
                "Invalid schema for s3 location: {scheme}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct S3Client<C: HttpClient> {
    client: C,
    credentials: AwsCredentials,
}

impl<C: HttpClient + 'static> S3Client<C> {
    pub fn new(client: C, credentials: AwsCredentials) -> Self {
        S3Client {
            client,
            credentials,
        }
    }

    fn authorize_request(&self, request: Request, region: &str) -> Result<Request> {
        let authorizer = AwsRequestAuthorizer {
            date: Utc::now(),
            credentials: &self.credentials,
            region,
        };

        authorizer.authorize(request)
    }

    pub fn file_source(&self, location: S3Location, region: &str) -> Result<Box<dyn FileSource>> {
        Ok(Box::new(S3Reader::new(
            self.client.clone(),
            location,
            self.credentials.clone(),
            region.to_string(),
        )))
    }

    pub fn list_prefix(
        &self,
        location: S3Location,
        region: &str,
    ) -> impl Stream<Item = Result<Vec<String>>> {
        let state = ListState {
            client: self.clone(),
            location,
            region: region.to_string(),
            continue_listing: true,
            continuation_token: None,
        };

        futures::stream::try_unfold(state, |state| async move {
            if !state.continue_listing {
                return Ok(None);
            }

            let (state, contents) = state.do_list_request().await?;
            let keys: Vec<_> = contents
                .into_iter()
                .map(|c| {
                    // TODO: Probably want to be little less cowboy-y about
                    // this.
                    c.key
                        .strip_prefix(&state.location.url.path()[1..])
                        .unwrap()
                        .strip_prefix('/')
                        .unwrap()
                        .to_string()
                })
                .collect();

            Ok(Some((keys, state)))
        })
    }
}

#[derive(Debug)]
struct ListState<C: HttpClient> {
    client: S3Client<C>,
    location: S3Location,
    region: String,
    continuation_token: Option<String>,
    continue_listing: bool,
}

impl<C: HttpClient + 'static> ListState<C> {
    async fn do_list_request(mut self) -> Result<(Self, Vec<S3ListContents>)> {
        // Object path only, does not include bucket. We also don't want to
        // include the leading slash.
        let prefix = &self.location.url.path()[1..];

        let mut url = self.location.url.clone();
        url.query_pairs_mut()
            .append_pair("list-type", "2")
            .append_pair("prefix", prefix);

        // Clear path, since it's not part of the request.
        url.path_segments_mut().unwrap().clear();

        if let Some(continuation_token) = self.continuation_token.take() {
            url.query_pairs_mut()
                .append_pair("continuation-token", &continuation_token);
        }

        let request = Request::new(Method::GET, url);
        let request = self.client.authorize_request(request, &self.region)?;

        let resp = self.client.client.do_request(request).await?;
        if resp.status() != StatusCode::OK {
            let text = read_text(resp).await?;
            return Err(RayexecError::new(format!("List error: {text}")));
        }

        let bytes = resp.bytes().await?;

        let list_resp: S3ListResponse = quick_xml::de::from_reader(bytes.reader())
            .context("failed to deserialize list response")?;

        self.continuation_token = list_resp.next_continuation_token;
        self.continue_listing = self.continuation_token.is_some();

        Ok((self, list_resp.contents))
    }
}

#[derive(Debug)]
pub struct S3Reader<C: HttpClient> {
    client: C,
    location: S3Location,
    credentials: AwsCredentials,
    region: String,
}

impl<C: HttpClient + 'static> S3Reader<C> {
    pub fn new(
        client: C,
        location: S3Location,
        credentials: AwsCredentials,
        region: String,
    ) -> Self {
        S3Reader {
            client,
            location,
            credentials,
            region,
        }
    }

    fn authorize_request(&self, request: Request) -> Result<Request> {
        let authorizer = AwsRequestAuthorizer {
            date: Utc::now(),
            credentials: &self.credentials,
            region: &self.region,
        };

        authorizer.authorize(request)
    }
}

impl<C: HttpClient + 'static> FileSource for S3Reader<C> {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        let range = format_range_header(start, start + len - 1);

        let mut request = Request::new(Method::GET, self.location.url.clone());
        request
            .headers_mut()
            .insert(RANGE, range.try_into().unwrap());

        let client = self.client.clone();
        let request = self.authorize_request(request);

        Box::pin(async move {
            let request = request?;
            let resp = client.do_request(request).await?;

            if resp.status() != StatusCode::PARTIAL_CONTENT {
                return Err(RayexecError::new("Server does not support range requests"));
            }

            resp.bytes().await.context("failed to get response body")
        })
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        let client = self.client.clone();
        let request = Request::new(Method::GET, self.location.url.clone());
        let request = self.authorize_request(request);

        let stream = stream::once(async move {
            let request = request?;
            let resp = client.do_request(request).await?;

            if resp.status() != StatusCode::OK {
                let text = read_text(resp).await?;
                return Err(RayexecError::new(format!("Stream results: {text}")));
            }

            Ok::<_, RayexecError>(resp.bytes_stream())
        })
        .try_flatten();

        stream.boxed()
    }

    fn size(&mut self) -> BoxFuture<Result<usize>> {
        let client = self.client.clone();
        let request = self.authorize_request(Request::new(Method::GET, self.location.url.clone()));

        Box::pin(async move {
            let request = request?;
            let resp = client.do_request(request).await?;

            if !resp.status().is_success() {
                let text = read_text(resp).await.unwrap_or_default();
                return Err(RayexecError::new(format!(
                    "Failed to get content-length: {text}"
                )));
            }

            let len = match resp.headers().get(CONTENT_LENGTH) {
                Some(header) => header
                    .to_str()
                    .context("failed to convert to string")?
                    .parse::<usize>()
                    .context("failed to parse content length")?,
                None => return Err(RayexecError::new("Response missing content-length header")),
            };

            Ok(len)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_s3_valid_location() {
        let expected = Url::parse("https://my_bucket.s3.us-east1.amazonaws.com/my/object").unwrap();
        let location =
            S3Location::from_url(Url::parse("s3://my_bucket/my/object").unwrap(), "us-east1")
                .unwrap();
        assert_eq!(expected, location.url)
    }

    #[test]
    fn parse_s3_invalid_location() {
        S3Location::from_url(Url::parse("gs://my_bucket/my/object").unwrap(), "us-east1")
            .unwrap_err();
    }
}
