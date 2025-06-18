use std::marker::PhantomData;

use glaredb_http::client::HttpClient;

use super::api::{CreateResponseRequest, CreateResponseResponse};

pub struct OpenAIClient<C: HttpClient> {
    api_key: String,
    client: C,
}

impl<C> OpenAIClient<C>
where
    C: HttpClient,
{
    pub fn create_response(
        req: CreateResponseRequest,
    ) -> OpenAIResponseFuture<CreateResponseResponse> {
        unimplemented!()
    }
}

pub struct OpenAIResponseFuture<R> {
    _resp: PhantomData<R>,
}
