//! Data source implementations.


use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use parser::options::StatementOptions;
use protogen::metastore::types::options::{
    CredentialsOptions,
    DatabaseOptions,
    TableOptions,
    TunnelOptions,
};
pub mod bigquery;
pub mod bson;
pub mod cassandra;
pub mod clickhouse;
pub mod common;
pub mod debug;
pub mod excel;
pub mod json;
pub mod lake;
pub mod lance;
pub mod mongodb;
pub mod mysql;
pub mod native;
pub mod object_store;
pub mod postgres;
pub mod snowflake;
pub mod sqlite;
pub mod sqlserver;


pub type DatasourceError = Box<dyn std::error::Error + Send + Sync>;
#[async_trait]
pub trait Datasource: Send + Sync {
    fn name(&self) -> &'static str;

    /// Validate the provided tunnel options.
    fn validate_tunnel_connections(
        &self,
        tunnel_opts: Option<&TunnelOptions>,
    ) -> Result<(), DatasourceError>;

    /// Validate the provided credentials.
    fn validate_credentials(
        &self,
        creds: Option<CredentialsOptions>,
    ) -> Result<(), DatasourceError>;

    /// Create a new datasource from the provided table options and credentials.
    /// CREATE EXTERNAL TABLE foo FROM <name> OPTIONS (...) [CREDENTIALS] (...) [TUNNEL] (...)
    // TODO: the datasource should have control over it's own CredentialsOptions and TunnelOptions
    fn table_options_from_stmt(
        &self,
        opts: &mut StatementOptions,
        creds: Option<CredentialsOptions>,
        tunnel_opts: Option<TunnelOptions>,
    ) -> Result<TableOptions, DatasourceError>;


    async fn create_table_provider(
        &self,
        tbl_options: &TableOptions,
        _tunnel_opts: Option<&TunnelOptions>,
    ) -> Result<Arc<dyn TableProvider>, DatasourceError>;

    /// Create a new datasource from the provided database options and credentials.
    /// If the datasource does not support databases, return `Ok(None)`.
    async fn table_provider_from_db_options(
        &self,
        schema: &str,
        name: &str,
        options: &DatabaseOptions,
        _tunnel_opts: Option<&TunnelOptions>,
    ) -> Result<Option<Arc<dyn TableProvider>>, DatasourceError>;
}
