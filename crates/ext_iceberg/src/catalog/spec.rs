use std::collections::HashMap;

use glaredb_error::{DbError, Result, ResultExt};
use glaredb_http::client::HttpClient;
use url::Url;

/// Icerberg REST catalog client.
///
/// Reference: <https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml>
#[derive(Debug)]
pub struct CatalogClient<C: HttpClient> {
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
    pub async fn configure(client: C) -> Result<Self> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct Endpoints {
    /// Base url to use, including a base path if any.
    ///
    /// The path should not have a trailing '/'.
    ///
    /// <https://host.com/path/>
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
            "https://o1.com/iceberg/v1/arn/namespaces/my_ns/tables/m_table",
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
