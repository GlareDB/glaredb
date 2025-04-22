#![allow(unused)]

use std::collections::HashMap;

use glaredb_error::{DbError, Result, ResultExt};
use glaredb_http::client::{HttpClient, HttpResponse, read_json_response, set_json_body};
use glaredb_http::{Method, Request, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use url::Url;

use super::spec::{
    CreateNamespaceRequest, CreateNamespaceResponse, CreateTableRequest, CreateTableResponse,
    ErrorModel, GetNamespaceResponse, ListNamespacesResponse, ListTablesResponse, LoadTableResponse,
    RenameTableRequest, RenameTableResponse, TableIdentifier, UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse, UpdateTableRequest, UpdateTableResponse,
};

/// Error returned by iceberg endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergErrorModel {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: String,
    pub code: i32,
}

/// Icerberg REST catalog client.
///
/// Reference: <https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml>
#[derive(Debug)]
pub struct CatalogClient<C: HttpClient> {
    endpoints: Endpoints,
    client: C,
    /// Catalog properties.
    ///
    /// Common: <https://iceberg.apache.org/docs/latest/configuration/#catalog-properties>
    properties: HashMap<String, String>,
}

impl<C> CatalogClient<C>
where
    C: HttpClient,
{
    /// Creates a new iceberg catalog client using the given http client.
    ///
    /// The catalog's 'config' endpoint will be queried to configure this
    /// client.
    pub async fn configure(client: C, base: Url, prefix: impl Into<String>) -> Result<Self> {
        let client = CatalogClient {
            endpoints: Endpoints::new(base, prefix),
            client,
            properties: HashMap::new(),
        };

        #[derive(Debug, Deserialize)]
        struct CatalogConfig {
            overrides: HashMap<String, String>,
            defaults: HashMap<String, String>,
            endpoints: Vec<String>,
        }

        // TODO
        let _conf: CatalogConfig = client
            .do_request::<(), _>(Method::GET, client.endpoints.v1_config()?, None)
            .await?;

        Ok(client)
    }

    async fn do_request<B, R>(&self, method: Method, url: Url, body: Option<B>) -> Result<R>
    where
        B: Serialize,
        R: DeserializeOwned,
    {
        let mut request = Request::new(method, url);
        if let Some(body) = body {
            set_json_body(&mut request, &body)?;
        }

        // Do the request!
        let resp = self.client.do_request(request).await?;

        if resp.status() != StatusCode::OK {
            // Error!
            let error_resp: ErrorModel = read_json_response(resp.into_bytes_stream())
                .await
                .context("Iceberg request failed; failed to read error message")?;

            return Err(
                DbError::new(format!("Iceberg catalog error: {}", error_resp.message))
                    .with_field("type", error_resp.error_type)
                    .with_field("code", error_resp.code),
            );
        }

        // Read the response!
        let resp = read_json_response(resp.into_bytes_stream()).await?;

        Ok(resp)
    }

    /// Create a namespace in the catalog.
    ///
    /// Reference: <https://iceberg.apache.org/spec/#create-a-namespace>
    pub async fn create_namespace(&self, namespace: impl Into<String>) -> Result<()> {
        let _resp: CreateNamespaceResponse = self
            .do_request(
                Method::POST,
                self.endpoints.v1_namespaces()?,
                Some(CreateNamespaceRequest {
                    // TODO: Support multi-level namespaces? Would definitely requires a bit of refactor...
                    namespace: vec![namespace.into()],
                    properties: HashMap::new(),
                }),
            )
            .await?;

        Ok(())
    }

    ///
    /// Reference: <https://iceberg.apache.org/spec/#list-namespaces>
    pub async fn list_namespaces(&self) -> Result<Vec<Vec<String>>> {
        let resp: ListNamespacesResponse = self
            .do_request::<(), _>(Method::GET, self.endpoints.v1_namespaces()?, None)
            .await?;

        Ok(resp.namespaces)
    }

    ///
    /// Reference: <https://iceberg.apache.org/spec/#get-namespace>
    pub async fn get_namespace(&self, namespace: impl Into<String>) -> Result<GetNamespaceResponse> {
        let namespace = namespace.into();
        let resp: GetNamespaceResponse = self
            .do_request::<(), _>(
                Method::GET,
                self.endpoints.v1_namespaces_namespace(&namespace)?,
                None,
            )
            .await?;

        Ok(resp)
    }

    /// Update namespace properties.
    ///
    /// Reference: <https://iceberg.apache.org/spec/#update-namespace-properties>
    pub async fn update_namespace_properties(
        &self,
        namespace: impl Into<String>,
        removals: Vec<String>,
        updates: HashMap<String, String>,
    ) -> Result<UpdateNamespacePropertiesResponse> {
        let namespace = namespace.into();
        let resp: UpdateNamespacePropertiesResponse = self
            .do_request(
                Method::POST,
                self.endpoints.v1_namespaces_namespace_properties(&namespace)?,
                Some(UpdateNamespacePropertiesRequest { removals, updates }),
            )
            .await?;

        Ok(resp)
    }

    ///
    /// Reference: <https://iceberg.apache.org/spec/#drop-namespace>
    pub async fn drop_namespace(&self, namespace: impl Into<String>) -> Result<()> {
        let namespace = namespace.into();
        let _: () = self
            .do_request::<(), _>(
                Method::DELETE,
                self.endpoints.v1_namespaces_namespace(&namespace)?,
                None,
            )
            .await?;

        Ok(())
    }

    ///
    /// Reference: <https://iceberg.apache.org/spec/#list-tables>
    pub async fn list_tables(&self, namespace: impl Into<String>) -> Result<Vec<TableIdentifier>> {
        let namespace = namespace.into();
        let resp: ListTablesResponse = self
            .do_request::<(), _>(
                Method::GET,
                self.endpoints.v1_namespaces_namespace_tables(&namespace)?,
                None,
            )
            .await?;

        Ok(resp.identifiers)
    }

    /// Create a table in a namespace.
    ///
    /// Reference: <https://iceberg.apache.org/spec/#create-table>
    pub async fn create_table(
        &self,
        namespace: impl Into<String>,
        request: CreateTableRequest,
    ) -> Result<CreateTableResponse> {
        let namespace = namespace.into();
        let resp: CreateTableResponse = self
            .do_request(
                Method::POST,
                self.endpoints.v1_namespaces_namespace_tables(&namespace)?,
                Some(request),
            )
            .await?;

        Ok(resp)
    }

    ///
    /// Reference: <https://iceberg.apache.org/spec/#load-table>
    pub async fn load_table(
        &self,
        namespace: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<LoadTableResponse> {
        let namespace = namespace.into();
        let table = table.into();
        let resp: LoadTableResponse = self
            .do_request::<(), _>(
                Method::GET,
                self.endpoints.v1_namespaces_namespace_tables_table(&namespace, &table)?,
                None,
            )
            .await?;

        Ok(resp)
    }

    ///
    /// Reference: <https://iceberg.apache.org/spec/#update-table>
    pub async fn update_table(
        &self,
        namespace: impl Into<String>,
        table: impl Into<String>,
        request: UpdateTableRequest,
    ) -> Result<UpdateTableResponse> {
        let namespace = namespace.into();
        let table = table.into();
        let resp: UpdateTableResponse = self
            .do_request(
                Method::POST,
                self.endpoints.v1_namespaces_namespace_tables_table(&namespace, &table)?,
                Some(request),
            )
            .await?;

        Ok(resp)
    }

    ///
    /// Reference: <https://iceberg.apache.org/spec/#drop-table>
    pub async fn drop_table(
        &self,
        namespace: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<()> {
        let namespace = namespace.into();
        let table = table.into();
        let _: () = self
            .do_request::<(), _>(
                Method::DELETE,
                self.endpoints.v1_namespaces_namespace_tables_table(&namespace, &table)?,
                None,
            )
            .await?;

        Ok(())
    }

    ///
    /// Reference: <https://iceberg.apache.org/spec/#rename-table>
    pub async fn rename_table(
        &self,
        source_namespace: impl Into<String>,
        source_table: impl Into<String>,
        dest_namespace: impl Into<String>,
        dest_table: impl Into<String>,
    ) -> Result<RenameTableResponse> {
        let source = TableIdentifier {
            namespace: vec![source_namespace.into()],
            name: source_table.into(),
        };
        let destination = TableIdentifier {
            namespace: vec![dest_namespace.into()],
            name: dest_table.into(),
        };

        let resp: RenameTableResponse = self
            .do_request(
                Method::POST,
                self.endpoints.v1_tables_rename()?,
                Some(RenameTableRequest {
                    source,
                    destination,
                }),
            )
            .await?;

        Ok(resp)
    }
}

#[derive(Debug)]
struct Endpoints {
    /// Base url to use, including a base path if any.
    ///
    /// The path should not have a trailing '/'.
    ///
    /// <https://host.com/path>
    base: Url,
    /// String to use when filling in the '{prefix}' portion of the path.
    ///
    /// For s3 tables, this should be the url-encoded bucket ARN.
    prefix: String,
}

impl Endpoints {
    fn new(base: Url, prefix: impl Into<String>) -> Self {
        Endpoints {
            base,
            prefix: prefix.into(),
        }
    }

    fn with_path<'a>(&self, segments: impl IntoIterator<Item = &'a str>) -> Result<Url> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| DbError::new("Cannot get path segments for url"))?
            .extend(segments);

        Ok(url)
    }

    fn v1_config(&self) -> Result<Url> {
        self.with_path(["v1", "config"])
    }

    fn v1_namespaces(&self) -> Result<Url> {
        self.with_path(["v1", &self.prefix, "namespaces"])
    }

    fn v1_namespaces_namespace(&self, namespace: &str) -> Result<Url> {
        self.with_path(["v1", &self.prefix, "namespaces", namespace])
    }

    fn v1_namespaces_namespace_properties(&self, namespace: &str) -> Result<Url> {
        self.with_path(["v1", &self.prefix, "namespaces", namespace, "properties"])
    }

    fn v1_namespaces_namespace_tables(&self, namespace: &str) -> Result<Url> {
        self.with_path(["v1", &self.prefix, "namespaces", namespace, "tables"])
    }

    fn v1_namespaces_namespace_tables_table(&self, namespace: &str, table: &str) -> Result<Url> {
        self.with_path(["v1", &self.prefix, "namespaces", namespace, "tables", table])
    }

    fn v1_tables_rename(&self) -> Result<Url> {
        self.with_path(["v1", &self.prefix, "tables", "rename"])
    }

    fn v1_namespaces_namespace_views(&self, namespace: &str) -> Result<Url> {
        self.with_path(["v1", &self.prefix, "namespaces", namespace, "views"])
    }

    fn v1_namespaces_namespace_views_view(&self, namespace: &str, view: &str) -> Result<Url> {
        self.with_path(["v1", &self.prefix, "namespaces", namespace, "views", view])
    }

    fn v1_views_rename(&self) -> Result<Url> {
        self.with_path(["v1", &self.prefix, "views", "rename"])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints() {
        let base = Url::parse("https://o1.com/iceberg").unwrap();
        let endpoints = Endpoints::new(base, "arn");

        assert_eq!(
            "https://o1.com/iceberg/v1/config",
            endpoints.v1_config().unwrap().as_str()
        );

        assert_eq!(
            "https://o1.com/iceberg/v1/arn/namespaces",
            endpoints.v1_namespaces().unwrap().as_str()
        );

        assert_eq!(
            "https://o1.com/iceberg/v1/arn/namespaces/my_ns",
            endpoints.v1_namespaces_namespace("my_ns").unwrap().as_str()
        );

        assert_eq!(
            "https://o1.com/iceberg/v1/arn/namespaces/my_ns/tables",
            endpoints
                .v1_namespaces_namespace_tables("my_ns")
                .unwrap()
                .as_str()
        );

        assert_eq!(
            "https://o1.com/iceberg/v1/arn/namespaces/my_ns/tables/my_table",
            endpoints
                .v1_namespaces_namespace_tables_table("my_ns", "my_table")
                .unwrap()
                .as_str()
        );

        assert_eq!(
            "https://o1.com/iceberg/v1/arn/tables/rename",
            endpoints.v1_tables_rename().unwrap().as_str()
        );

        assert_eq!(
            "https://o1.com/iceberg/v1/arn/namespaces/my_ns/views",
            endpoints
                .v1_namespaces_namespace_views("my_ns")
                .unwrap()
                .as_str()
        );

        assert_eq!(
            "https://o1.com/iceberg/v1/arn/namespaces/my_ns/views/my_view",
            endpoints
                .v1_namespaces_namespace_views_view("my_ns", "my_view")
                .unwrap()
                .as_str()
        );

        assert_eq!(
            "https://o1.com/iceberg/v1/arn/views/rename",
            endpoints.v1_views_rename().unwrap().as_str()
        );
    }
}
