//! Delta lake catalog implementations.
//!
//! Most of this was copied in from the `deltalake` crate to make some
//! modifications with how we construct clients, and what errors get returned.
use crate::delta::errors::{DeltaError, Result};
use async_trait::async_trait;
use reqwest::header;
use serde::Deserialize;

#[async_trait]
pub trait DataCatalog: Sync + Send {
    /// Get the storage location for a given table.
    async fn get_table_storage_location(
        &self,
        database_name: &str, // "schema"
        table_name: &str,
    ) -> Result<String>;
}

/// Databricks Unity Catalog - implementation of the `DataCatalog` trait
#[derive(Debug, Clone)]
pub struct UnityCatalog {
    client: reqwest::Client,
    workspace_url: String,
    catalog_id: String,
}

impl UnityCatalog {
    pub async fn connect(
        access_token: &str,
        workspace_url: &str,
        catalog_id: &str,
    ) -> Result<Self> {
        let auth_header_val = header::HeaderValue::from_str(&format!("Bearer {}", &access_token))
            .map_err(|_| DeltaError::Static("Invalid Databricks access token"))?;

        let headers = header::HeaderMap::from_iter([(header::AUTHORIZATION, auth_header_val)]);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        // Check that we can reach the databricks workspace.
        let _resp = client
            .get(format!("{}/api/2.1/unity-catalog/catalogs", workspace_url))
            .send()
            .await?;

        Ok(Self {
            client,
            workspace_url: workspace_url.to_string(),
            catalog_id: catalog_id.to_string(),
        })
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TableResponse {
    Success { storage_location: String },
    Error { error_code: String, message: String },
}

#[async_trait]
impl DataCatalog for UnityCatalog {
    /// Get the table storage location from the UnityCatalog
    async fn get_table_storage_location(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<String> {
        let resp = self
            .client
            .get(format!(
                "{}/api/2.1/unity-catalog/tables/{}.{}.{}",
                &self.workspace_url, self.catalog_id, database_name, table_name
            ))
            .send()
            .await?;

        let parsed_resp: TableResponse = resp.json().await?;
        match parsed_resp {
            TableResponse::Success { storage_location } => Ok(storage_location),
            TableResponse::Error {
                error_code,
                message,
            } => Err(DeltaError::UnityInvalidTable {
                error_code,
                message,
            }),
        }
    }
}
