use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{FutureExt, StreamExt};
use glaredb_core::util::marker::PhantomCovariant;
use glaredb_error::{Result, ResultExt};
use glaredb_http::client::{HttpClient, HttpResponse};
use glaredb_http::{Method, Request};
use serde::Deserialize;
use url::Url;

use super::api::{CreateResponseRequest, CreateResponseResponse};

pub const OPEN_AI_ENDPOINTS: Endpoints = Endpoints {
    root: "https://api.openai.com",
    create_model_response: "/v1/responses",
};

#[derive(Debug, Clone, Copy)]
pub struct Endpoints {
    pub root: &'static str,
    pub create_model_response: &'static str,
}

pub struct OpenAIClient<C: HttpClient> {
    api_key: String,
    client: C,
    endpoints: Endpoints,
}

impl<C> OpenAIClient<C>
where
    C: HttpClient,
{
    pub fn create_model_response(
        &self,
        req: CreateResponseRequest,
    ) -> Result<OpenAIRequestFuture<C, CreateResponseResponse>> {
        // let req = Request::new(Method::POST, url)
        unimplemented!()
    }
}

pub struct OpenAIRequestFuture<'a, C: HttpClient, R: Deserialize<'a>> {
    client: &'a C,
    buf: Vec<u8>,
    state: RequestState<C>,
    _resp: PhantomCovariant<R>,
}

enum RequestState<C: HttpClient> {
    /// We're making the request.
    Requesting { req_fut: C::RequestFuture },
    /// We're reading the response.
    Reading {
        stream: <C::Response as HttpResponse>::BytesStream,
    },
}

impl<'a, C, R> Future for OpenAIRequestFuture<'a, C, R>
where
    C: HttpClient,
    R: Deserialize<'a>,
{
    type Output = Result<R>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                RequestState::Requesting { req_fut } => {
                    match req_fut.poll_unpin(cx)? {
                        Poll::Ready(resp) => {
                            this.state = RequestState::Reading {
                                stream: resp.into_bytes_stream(),
                            };
                            // Continue...
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                RequestState::Reading { stream } => match stream.poll_next_unpin(cx)? {
                    Poll::Ready(Some(bs)) => {
                        this.buf.extend_from_slice(bs.as_ref());
                        // Continue...
                    }
                    Poll::Ready(None) => {
                        // // Stream finished, deserialize.
                        // return Poll::Ready(
                        //     serde_json::from_slice(&this.buf)
                        //         .context("Failed to deserialize response from OpenAI"),
                        // );
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}
