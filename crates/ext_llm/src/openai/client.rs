use std::pin::Pin;
use std::task::{Context, Poll};

use futures::FutureExt;
use glaredb_error::{Result, ResultExt};
use glaredb_http::client::{HttpClient, HttpResponse, set_authorization_bearer, set_json_body};
use glaredb_http::response_buffer::{ReadStreamJsonFuture, ResponseBuffer};
use glaredb_http::{Method, Request};
use serde::{Deserialize, Serialize};
use url::Url;

use super::api::{CreateResponseRequest, CreateResponseResponse};

pub const OPEN_AI_ENDPOINTS: Endpoints = Endpoints {
    create_model_response: "https://api.openai.com/v1/responses",
};

#[derive(Debug, Clone, Copy)]
pub struct Endpoints {
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
    pub fn create_model_response<'a>(
        &'a self,
        req: CreateResponseRequest<'a>,
    ) -> OpenAIRequestFuture<'a, C, CreateResponseRequest<'a>, CreateResponseResponse<'a>> {
        OpenAIRequestFuture {
            state: RequestState::Init {
                api_key: &self.api_key,
                req: Some(req),
                client: &self.client,
                endpoints: &self.endpoints,
            },
        }
    }
}

pub struct OpenAIRequestFuture<'a, C: HttpClient, Req, Resp> {
    state: RequestState<'a, C, Req, Resp>,
}

enum RequestState<'a, C: HttpClient, Req, Resp> {
    /// Initialize the request.
    Init {
        api_key: &'a str,
        req: Option<Req>,
        client: &'a C,
        endpoints: &'a Endpoints,
    },
    /// We're making the request.
    Requesting { req_fut: C::RequestFuture },
    /// We're reading the response.
    Reading {
        fut: ReadStreamJsonFuture<Resp, <C::Response as HttpResponse>::BytesStream>,
    },
}

impl<'a, C, Req, Resp> Future for OpenAIRequestFuture<'a, C, Req, Resp>
where
    C: HttpClient,
    Req: Serialize + Unpin,
    Resp: for<'de> Deserialize<'de>,
{
    type Output = Result<ResponseBuffer<Resp>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                RequestState::Init {
                    api_key,
                    client,
                    endpoints,
                    req,
                } => {
                    let url = match Url::parse(endpoints.create_model_response)
                        .context("Failed to parse OpenAI url")
                    {
                        Ok(url) => url,
                        Err(e) => return Poll::Ready(Err(e)),
                    };
                    let body = req.take().expect("request body to be Some");
                    let mut req = Request::new(Method::POST, url);
                    set_authorization_bearer(&mut req, api_key)?;
                    set_json_body(&mut req, &body)?;
                    let req_fut = client.do_request(req);

                    this.state = RequestState::Requesting { req_fut }
                    // Continue...
                }
                RequestState::Requesting { req_fut } => {
                    match req_fut.poll_unpin(cx)? {
                        Poll::Ready(resp) => {
                            this.state = RequestState::Reading {
                                fut: ResponseBuffer::read_stream_json(
                                    Vec::new(),
                                    resp.into_bytes_stream(),
                                ),
                            };
                            // Continue...
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                RequestState::Reading { fut } => return fut.poll_unpin(cx),
            }
        }
    }
}
