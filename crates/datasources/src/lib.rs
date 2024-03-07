//! Data source implementations.


use parser::errors::ParserError;
use parser::options::StatementOptions;
use protogen::metastore::types::options::{CredentialsOptions, TableOptionsImpl, TunnelOptions};
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

pub trait Datasource {
    /// Returns the name for this datasource. This is used in the SQL syntax,
    /// for example. CREATE EXTERNAL TABLE foo FROM <name> OPTIONS (...)
    const NAME: &'static str;
    type TableOptions: TableOptionsImpl;

    /// Create a new datasource from the provided table options and credentials.
    /// CREATE EXTERNAL TABLE foo FROM <name> OPTIONS (...) [CREDENTIALS] (...) [TUNNEL] (...)
    fn table_options_from_stmt(
        opts: &mut StatementOptions,
        creds: Option<CredentialsOptions>,
        tunnel_opts: Option<TunnelOptions>,
    ) -> Result<Self::TableOptions, ParserError>
    where
        Self: Sized;
}
