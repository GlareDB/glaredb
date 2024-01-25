use std::{collections::HashMap, io::Read, time::Duration};

use flate2::read::GzDecoder;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE},
    Client, IntoUrl, StatusCode, Url,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::{trace, warn};
use uuid::Uuid;

use crate::{
    auth::Token,
    errors::{Result, SnowflakeError},
};

const APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));
const BODY_CONTENT_TYPE: &str = "application/json";
const REQ_ACCEPT: &str = "application/snowflake";

const HEADER_SSE_C_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
const HEADER_SSE_C_AES_VALUE: &str = "AES256";
const HEADER_SSE_C_KEY: &str = "x-amz-server-side-encryption-customer-key";

#[derive(Debug)]
pub struct RequestId(Uuid);

impl RequestId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Serialize for RequestId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = Uuid::encode_buffer();
        serializer.serialize_str(self.0.hyphenated().encode_lower(&mut buf))
    }
}

#[derive(Debug, Default)]
pub struct SnowflakeClientBuilder {
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
}

impl SnowflakeClientBuilder {
    #[allow(unused)]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    #[allow(unused)]
    pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = Some(connect_timeout);
        self
    }

    pub fn build<U: IntoUrl>(self, base_url: U) -> Result<SnowflakeClient> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static(BODY_CONTENT_TYPE));
        default_headers.insert(ACCEPT, HeaderValue::from_static(REQ_ACCEPT));

        let mut builder = Client::builder()
            .user_agent(APP_USER_AGENT)
            .default_headers(default_headers);

        if let Some(timeout) = self.timeout {
            builder = builder.timeout(timeout);
        }

        if let Some(connect_timeout) = self.connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }

        let client = builder.build()?;
        Ok(SnowflakeClient {
            base_url: base_url.into_url()?,
            inner: client,
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EmptySerde {}

impl EmptySerde {
    const EMPTY_SERDE: Self = Self {};

    pub fn new() -> &'static Self {
        &Self::EMPTY_SERDE
    }

    pub fn none() -> Option<&'static Self> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecMethod {
    Post,
    Get,
}

#[derive(Debug, Clone)]
pub struct SnowflakeClient {
    base_url: Url,
    inner: Client,
}

impl SnowflakeClient {
    pub fn builder() -> SnowflakeClientBuilder {
        SnowflakeClientBuilder::default()
    }

    pub async fn execute<P, B, R>(
        &self,
        method: ExecMethod,
        url: &str,
        // Optional params because they can already be in URL
        params: Option<&P>,
        body: &B,
        token: Option<&Token>,
    ) -> Result<R>
    where
        P: Serialize,
        B: Serialize,
        R: DeserializeOwned,
    {
        let url = self
            .base_url
            .join(url)
            // The URL crate we use is from the "reqwest" crate which doesn't
            // expose the error and hence we cast it to a string.
            .map_err(|e| SnowflakeError::UrlParseError(format!("{e}")))?;

        let mut req = match method {
            ExecMethod::Post => self.inner.post(url).json(&body),
            ExecMethod::Get => self.inner.get(url),
        };

        if let Some(params) = params {
            req = req.query(&params);
        }

        if let Some(token) = token {
            let val = format!("Snowflake Token=\"{}\"", token.value());
            req = req.header(
                AUTHORIZATION,
                HeaderValue::from_str(&val)
                    .expect("snowflake returned token must be a valid header value"),
            );
        }

        const MAX_RETRY_COUNT: usize = 3;

        fn should_retry(count: usize, status: StatusCode) -> bool {
            let should_not_retry =
                // If the try count exceeds the limit
                count >= MAX_RETRY_COUNT
                // If the request is successful
                || status.is_success()
                // or if it's a client error != 429
                || (status.is_client_error() && status != StatusCode::TOO_MANY_REQUESTS);

            !should_not_retry
        }

        for try_count in 0..=MAX_RETRY_COUNT {
            let res = req
                .try_clone()
                .expect("request should be clone-able")
                .send()
                .await?;

            let status = res.status();

            if should_retry(try_count, status) {
                let retry = try_count + 1;
                warn!(%retry, %status, "request failed, retrying!");
                continue;
            }

            if !status.is_success() {
                // TODO: Parse error JSON and return with proper error message.
                return Err(SnowflakeError::HttpError(res.status()));
            }

            let res = res.text().await?;
            trace!(%res, "response");

            let res: R = serde_json::from_str(&res)?;
            return Ok(res);
        }

        // This is technically unreachable but just to be safe (to avoid panics
        // in prod) just debug-asserting here.
        debug_assert!(
            false,
            "unreachable: request re-tried even after max count exceeded"
        );
        Err(SnowflakeError::HttpError(StatusCode::INTERNAL_SERVER_ERROR))
    }
}

#[derive(Debug, Clone)]
pub struct SnowflakeChunkDl {
    inner: Client,
}

impl SnowflakeChunkDl {
    pub fn new(headers: HashMap<String, String>, qrmk: String) -> Result<Self> {
        let headers: HeaderMap = if headers.is_empty() {
            let mut headers = HeaderMap::with_capacity(2);
            headers.insert(
                HEADER_SSE_C_ALGORITHM,
                HeaderValue::from_static(HEADER_SSE_C_AES_VALUE),
            );
            headers.insert(
                HEADER_SSE_C_KEY,
                HeaderValue::from_str(&qrmk).expect("qrmk value must be valid"),
            );
            headers
        } else {
            headers
                .into_iter()
                .map(|(k, v)| {
                    (
                        HeaderName::from_bytes(k.as_bytes())
                            .expect("chunk header name should be valid"),
                        HeaderValue::from_str(&v).expect("chunk header value should be valid"),
                    )
                })
                .collect()
        };

        let inner = Client::builder().default_headers(headers).build()?;
        Ok(Self { inner })
    }

    pub async fn download(&self, url: &str) -> Result<Vec<u8>> {
        let res = self.inner.get(url).send().await?;
        if !res.status().is_success() {
            return Err(SnowflakeError::HttpError(res.status()));
        }
        let res = res.bytes().await?;
        let res = if res[0] == 0x1f && res[1] == 0x8b {
            // Gzip format, need to decode
            let mut gz = GzDecoder::new(&res[..]);
            let mut decoded = Vec::new();
            gz.read_to_end(&mut decoded)?;
            decoded
        } else {
            res.to_vec()
        };
        Ok(res)
    }
}
