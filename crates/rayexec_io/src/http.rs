use futures::Future;
use rayexec_error::{RayexecError, Result, ResultExt};
use reqwest::{
    header::{CONTENT_LENGTH, RANGE},
    StatusCode,
};
use url::Url;

use crate::AsyncReadAt;

#[derive(Debug, Clone)]
pub struct HttpClient {
    client: reqwest::Client,
    handle: tokio::runtime::Handle,
}

impl HttpClient {
    pub fn new(handle: tokio::runtime::Handle) -> Self {
        HttpClient {
            client: reqwest::Client::new(),
            handle,
        }
    }

    pub fn with_reqwest_client(client: reqwest::Client, handle: tokio::runtime::Handle) -> Self {
        HttpClient { client, handle }
    }

    pub fn reader(&self, url: Url) -> HttpFileReader {
        HttpFileReader {
            client: self.client.clone(),
            handle: self.handle.clone(),
            url,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HttpFileReader {
    client: reqwest::Client,
    handle: tokio::runtime::Handle,
    url: Url,
}

impl HttpFileReader {
    pub async fn content_length(&self) -> Result<usize> {
        let send_fut = self.client.head(self.url.as_str()).send();
        self.handle
            .spawn(async move {
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
                    None => {
                        return Err(RayexecError::new("Response missing content-length header"))
                    }
                };

                Ok(len)
            })
            .await
            .expect("join")
    }

    fn format_range_header(start: usize, end: usize) -> String {
        format!("bytes={start}-{end}")
    }

    async fn read_at_inner(&mut self, start: usize, buf: &mut [u8]) -> Result<()> {
        let range = Self::format_range_header(start, start + buf.len() - 1);

        let send_fut = self
            .client
            .get(self.url.as_str())
            .header(RANGE, range)
            .send();

        let body_bytes = self
            .handle
            .spawn(async move {
                let resp = send_fut.await.context("failed to send GET request")?;

                if resp.status() != StatusCode::PARTIAL_CONTENT {
                    return Err(RayexecError::new("Server does not support range requests"));
                }

                resp.bytes().await.context("failed to get response body")
            })
            .await
            .expect("join")?;

        buf.copy_from_slice(&body_bytes);

        Ok(())
    }
}

impl AsyncReadAt for HttpFileReader {
    fn read_at(&mut self, start: usize, buf: &mut [u8]) -> impl Future<Output = Result<()>> + Send {
        self.read_at_inner(start, buf)
    }
}
