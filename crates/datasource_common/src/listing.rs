//! Module for providing virtual schema listings for data sources.
//!
//! Virtual listers can list schema and table information about the underlying
//! data source. These essentially provide a trimmed down information schema.
use crate::errors::Result;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct VirtualSchemas {
    pub schema_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct VirtualTables {
    // These two vectors should always be the same length. Keeping the data as
    // vectors instead of create a vector of structs makes conversion into arrow
    // arrays easier.
    pub table_schemas: Vec<String>,
    pub table_names: Vec<String>,
}

#[async_trait]
pub trait VirtualLister: Sync + Send {
    /// List schemas for a data source.
    async fn list_schemas(&self) -> Result<VirtualSchemas>;

    /// List tables for a data source.
    async fn list_tables(&self) -> Result<VirtualTables>;
}

#[derive(Debug, Clone, Copy)]
pub struct EmptyLister;

#[async_trait]
impl VirtualLister for EmptyLister {
    async fn list_schemas(&self) -> Result<VirtualSchemas> {
        Ok(VirtualSchemas {
            schema_names: Vec::new(),
        })
    }

    async fn list_tables(&self) -> Result<VirtualTables> {
        Ok(VirtualTables {
            table_schemas: Vec::new(),
            table_names: Vec::new(),
        })
    }
}
