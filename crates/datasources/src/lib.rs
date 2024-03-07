//! Data source implementations.


use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use parser::errors::ParserError;
use parser::options::StatementOptions;
use protogen::metastore::types::catalog::TableEntry;
use protogen::metastore::types::options::{CredentialsOptions, TableOptions, TunnelOptions};
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

#[async_trait]
pub trait Datasource: Sized {
    /// Returns the name for this datasource. This is used in the SQL syntax,
    /// for example. CREATE EXTERNAL TABLE foo FROM <name> OPTIONS (...)
    const NAME: &'static str;


    /// Create a new datasource from the provided table options and credentials.
    /// CREATE EXTERNAL TABLE foo FROM <name> OPTIONS (...) [CREDENTIALS] (...) [TUNNEL] (...)
    fn table_options_from_stmt(
        opts: &mut StatementOptions,
        creds: Option<CredentialsOptions>,
        tunnel_opts: Option<TunnelOptions>,
    ) -> Result<impl Into<TableOptions>, ParserError>
    where
        Self: Sized;
        
    async fn dispatch_table_entry<E>(entry: &TableEntry) -> Result<Arc<dyn TableProvider>, E> {
        Self::dispatch_table_entry_with_tunnel(entry, None).await
    }
    async fn dispatch_table_entry_with_tunnel<E>(
        entry: &TableEntry,
        tunnel_opts: Option<&TunnelOptions>,
    ) -> Result<Arc<dyn TableProvider>, E> {
        unimplemented!()
    }
}
