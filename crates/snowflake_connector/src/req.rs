use std::time::Duration;

use reqwest::{
    header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE},
    Client, IntoUrl, Url,
};
use serde::{de::DeserializeOwned, Serialize};
use tracing::trace;
use uuid::Uuid;

use crate::{
    auth::Token,
    errors::{Result, SnowflakeError},
};

const APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));
const BODY_CONTENT_TYPE: &str = "application/json";
const REQ_ACCEPT: &str = "application/snowflake";

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
        url: &str,
        params: &P,
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

        let mut req = self.inner.post(url).query(&params).json(&body);
        if let Some(token) = token {
            let val = format!("Snowflake Token=\"{}\"", token.value());
            req = req.header(
                AUTHORIZATION,
                HeaderValue::from_str(&val)
                    .expect("snowflake returned token must be a valid header value"),
            );
        }

        let res = req.send().await?;
        if !res.status().is_success() {
            return Err(SnowflakeError::HttpError(res.status()));
        }

        let res = res.text().await?;
        trace!(%res, "response");

        let res: R = serde_json::from_str(&res)?;
        Ok(res)
    }
}
