use std::fmt::Debug;

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use glaredb_error::{Result, ResultExt};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Request, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;

pub trait HttpClient: Sync + Send + Debug + Clone + 'static {
    type Response: HttpResponse;
    type RequestFuture: Future<Output = Result<Self::Response>> + Sync + Send + Unpin;

    /// Do the request.
    fn do_request(&self, request: Request) -> Self::RequestFuture;
}

pub trait HttpResponse: Sync + Send {
    type BytesStream: Stream<Item = Result<Bytes>> + Sync + Send + Unpin;

    fn status(&self) -> StatusCode;
    fn headers(&self) -> &HeaderMap;

    /// Convert the response body into a byte stream.
    fn into_bytes_stream(self) -> Self::BytesStream;
}

/// Helper to set a json body on this request.
///
/// Overwrites the existing body and 'Content-Type' of the request.
pub fn set_json_body<T>(request: &mut Request, body: &T) -> Result<()>
where
    T: Serialize + ?Sized,
{
    let body = serde_json::to_vec(body).context("Failed to serialize request body to json")?;
    *request.body_mut() = Some(body.into());
    request
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    Ok(())
}

/// Helper to set a form body on this request.
///
/// Overwrites the existing body and 'Content-Type' of the request.
pub fn set_form_body<T>(request: &mut Request, body: &T) -> Result<()>
where
    T: Serialize + ?Sized,
{
    let body = serde_urlencoded::to_string(body)
        .context("Failed to serialize request body to url encoded form")?;
    *request.body_mut() = Some(body.into());
    request.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded"),
    );

    Ok(())
}

/// Helper to read a json response from a byte stream.
///
/// This will collect the full response before trying to deserialize it.
pub async fn read_json_response<T, S>(mut stream: S) -> Result<T>
where
    T: DeserializeOwned,
    S: Stream<Item = Result<Bytes>> + Sync + Send + Unpin,
{
    let mut bytes = Vec::new();
    while let Some(resp) = stream.try_next().await? {
        bytes.extend_from_slice(resp.as_ref());
    }
    serde_json::from_slice(&bytes).context("Failed to deserialize response body as json")
}
