use std::collections::HashMap;
use std::marker::PhantomData;

use futures::future::BoxFuture;
use futures::{stream, Stream};
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::database::catalog_entry::TableEntry;
use rayexec_execution::database::memory_catalog::MemoryCatalog;
use rayexec_execution::datasource::take_option;
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::catalog_storage::CatalogStorage;
use rayexec_io::http::reqwest::{Method, Request, StatusCode};
use rayexec_io::http::{read_json, read_text, HttpClient, HttpResponse};
use serde::de::DeserializeOwned;
use url::Url;

use crate::rest::{UnityListSchemasResponse, UnityListTablesResponse};

/// Key for specifying the endpoint to connect to.
pub const ENDPOINT_OPTION_KEY: &str = "endpoint";

#[derive(Debug, Clone)]
pub struct UnityCatalogConnection<R: Runtime> {
    runtime: R,
    client: R::HttpClient,
    /// Configured endpoint we'll be using for all requests.
    endpoint: Url,
}

impl<R: Runtime> UnityCatalogConnection<R> {
    pub async fn connect(
        runtime: R,
        mut options: HashMap<String, OwnedScalarValue>,
    ) -> Result<Self> {
        let endpoint = take_option(ENDPOINT_OPTION_KEY, &mut options)?.try_into_string()?;
        let endpoint = Url::parse(&endpoint).context("failed to parse endpoint")?;

        let client = runtime.http_client();

        // TODO: Probably a request to ensure endpoint actually exists.

        Ok(UnityCatalogConnection {
            runtime,
            client,
            endpoint,
        })
    }

    pub fn list_schemas(
        &self,
        catalog_name: &str,
    ) -> Result<UnityListStream<R::HttpClient, UnityListSchemasResponse>> {
        let mut url = self
            .endpoint
            .join("/api/2.1/unity-catalog/schemas")
            .context("failed to build url")?;

        url.query_pairs_mut()
            .append_pair("catalog_name", catalog_name);

        Ok(UnityListStream::new(self.client.clone(), url))
    }

    pub fn list_tables(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<UnityListStream<R::HttpClient, UnityListTablesResponse>> {
        let mut url = self
            .endpoint
            .join("/api/2.1/unity-catalog/tables")
            .context("failed to build url")?;

        url.query_pairs_mut()
            .append_pair("catalog_name", catalog_name);
        url.query_pairs_mut()
            .append_pair("schema_name", schema_name);

        Ok(UnityListStream::new(self.client.clone(), url))
    }
}

impl<R: Runtime> CatalogStorage for UnityCatalogConnection<R> {
    fn initial_load(&self, catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>> {
        unimplemented!()
    }

    fn persist(&self, catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>> {
        unimplemented!()
    }

    fn load_table(&self, schema: &str, name: &str) -> BoxFuture<'_, Result<Option<TableEntry>>> {
        unimplemented!()
    }
}

/// Trait that should be implemented on every list response.
pub trait ListResponseBody: DeserializeOwned + Sync + Send + 'static {
    /// Get the next page token if it's set.
    fn next_page_token(&self) -> Option<&str>;
}

/// Common list stream for all unity list endpoints.
///
/// Note that everything is configured as query params in the url (e.g. catalog
/// name) so no body.
pub struct UnityListStream<C: HttpClient, R: ListResponseBody> {
    client: C,
    /// Url we're making the request to.
    url: Url,
    /// Token to get the next set of results.
    page_token: Option<String>,
    /// If we've already made a request.
    did_request: bool,
    _resp: PhantomData<R>,
}

impl<C, R> UnityListStream<C, R>
where
    C: HttpClient,
    R: ListResponseBody,
{
    fn new(client: C, url: Url) -> Self {
        UnityListStream {
            client,
            url,
            page_token: None,
            did_request: false,
            _resp: PhantomData,
        }
    }

    /// Read the next set of results.
    ///
    /// Returns Ok(None) once the list has been exhausted.
    pub async fn read_next(&mut self) -> Result<Option<R>> {
        if self.did_request && self.page_token.is_none() {
            // Nothing more the request.
            return Ok(None);
        }

        let mut url = self.url.clone();
        // Set page token to get the next page in the list if we need to.
        if let Some(page_token) = &self.page_token {
            url.query_pairs_mut().append_pair("page_token", page_token);
        }

        let req = Request::new(Method::GET, self.url.clone());
        let resp = self.client.do_request(req).await?;
        if resp.status() != StatusCode::OK {
            let status = resp.status();
            match read_text(resp).await {
                Ok(text) => {
                    return Err(RayexecError::new(format!(
                        "Expect 200 OK, got {status}. Response text: {text}",
                    )))
                }
                Err(_) => {
                    // TODO: Do something with the error.
                    return Err(RayexecError::new(format!("Expect 200 OK, got {status}",)));
                }
            }
        }

        let resp = read_json::<R>(resp).await?;

        self.page_token = resp.next_page_token().map(|s| s.to_string());
        self.did_request = true;

        Ok(Some(resp))
    }

    /// Turns self into an async stream returning response bodies.
    pub fn into_stream(self) -> impl Stream<Item = Result<R>> {
        stream::unfold(self, |mut state| async move {
            match state.read_next().await {
                Ok(None) => None,
                Ok(Some(resp)) => Some((Ok(resp), state)),
                Err(e) => Some((Err(e), state)),
            }
        })
    }
}
