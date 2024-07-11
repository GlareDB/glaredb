use bytes::Bytes;
use rayexec_error::{RayexecError, Result, ResultExt};
use reqwest::{
    header::{CONTENT_LENGTH, RANGE},
    StatusCode,
};
use std::fmt::Debug;
use tracing::debug;
use url::Url;

use crate::FileSource;

pub trait HttpClient: Debug + Sync + Send + 'static {
    /// Create a reader for the given url.
    fn reader(&self, url: Url) -> Box<dyn FileSource>;
}

/// Shared logic for http clients implemented on top of reqwest.
///
/// This should not be instantiated directly, and instead client should be
/// generated through the execution runtime.
///
/// This exists because reqwest has two implementations; one using tokio and
/// another using web-sys bindings for wasm, with the implementation depending
/// on the target triple the binary is being built for. So while it looks like
/// we're using just "reqwest", we're actually using a tokio flavored reqwest,
/// and a web-sys flavored request.
///
/// So why do we need to go through the runtime? We'll unfortunately the tokio
/// flavored reqwest has a dependency on a tokio runtime context, and by going
/// through the runtime, we're able to "wrap" these methods in a tokio context.
/// The web-sys flavor doesn't require any wrapping, but it has it's own set of
/// problems with certain wasm-bindgen things not being `Send`. So we can't
/// really have a single abstraction here that wraps depending on if a have a
/// tokio handle or not (since compilation will fail on the wasm-bindgen future
/// not being send).
#[derive(Debug, Clone, Default)]
pub struct ReqwestClient(reqwest::Client);

impl ReqwestClient {
    pub fn reader(&self, url: Url) -> ReqwestClientReader {
        ReqwestClientReader {
            client: self.0.clone(),
            url,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReqwestClientReader {
    pub client: reqwest::Client,
    pub url: Url,
}

impl ReqwestClientReader {
    pub async fn content_length(&self) -> Result<usize> {
        debug!(url = %self.url, "http getting content length");

        let send_fut = self.client.head(self.url.as_str()).send();
        let resp = send_fut.await.context("failed to send HEAD request")?;

        if !resp.status().is_success() {
            return Err(RayexecError::new("Failed to get content-length"));
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
    }

    pub async fn read_range(&mut self, start: usize, len: usize) -> Result<Bytes> {
        debug!(url = %self.url, %start, %len, "http reading range");

        let range = Self::format_range_header(start, start + len - 1);
        let send_fut = self
            .client
            .get(self.url.as_str())
            .header(RANGE, range)
            .send();
        let resp = send_fut.await.context("failed to send GET request")?;

        if resp.status() != StatusCode::PARTIAL_CONTENT {
            return Err(RayexecError::new("Server does not support range requests"));
        }

        resp.bytes().await.context("failed to get response body")
    }

    fn format_range_header(start: usize, end: usize) -> String {
        format!("bytes={start}-{end}")
    }
}
