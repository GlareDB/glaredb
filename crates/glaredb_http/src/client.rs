use std::fmt::Debug;

use bytes::Bytes;
use futures::Stream;
use glaredb_error::Result;
use reqwest::header::HeaderMap;
use reqwest::{Request, StatusCode};

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
