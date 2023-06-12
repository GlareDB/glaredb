//! Module for providing virtual schema listings for data sources.
//!
//! Virtual listers can list schema and table information about the underlying
//! data source. These essentially provide a trimmed down information schema.

use async_trait::async_trait;

use super::errors::Result;

#[derive(Debug, Clone)]
pub struct VirtualTable {
    pub schema: String,
    pub table: String,
}

#[async_trait]
pub trait VirtualLister: Sync + Send {
    /// List schemas for a data source.
    async fn list_schemas(&self) -> Result<Vec<String>>;

    /// List tables for a data source.
    async fn list_tables(&self, schema: &str) -> Result<Vec<String>>;
}
