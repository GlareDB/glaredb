use std::fmt::Debug;

use bytes::Bytes;
use glaredb_error::Result;
use reqwest::header::{HeaderMap, RANGE};
use reqwest::{Method, Request, Response, StatusCode};

pub trait HttpClient: Sync + Send + Debug + Clone + 'static {
    type Response: HttpResponse;
    type RequestFuture: Future<Output = Result<Self::Response>> + Sync + Send + Unpin;

    /// Do the request.
    fn do_request(&self, request: Request) -> Self::RequestFuture;
}

pub trait HttpResponse: Sync + Send {
    type ChunkFuture: Future<Output = Result<Option<Bytes>>> + Sync + Send + Unpin;

    fn status(&self) -> StatusCode;
    fn headers(&self) -> &HeaderMap;

    /// Stream a chunk of the response body.
    ///
    /// The future resolves to Ok(None) when there's no more bytes left to
    /// stream.
    fn chunk(&mut self) -> Self::ChunkFuture;
}
