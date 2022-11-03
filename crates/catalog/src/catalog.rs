use crate::errors::{CatalogError, Result};
use catalog_types::keys::{PartitionId, SchemaId, TableId};
use datafusion::arrow::datatypes::Schema;
use std::sync::Arc;

/// The top-level catalog.
pub struct DatabaseCatalog {}

impl DatabaseCatalog {
    /// Open up a catalog to some persistent storage.
    pub async fn connect_storage() -> Result<DatabaseCatalog> {
        unimplemented!()
    }

    /// Try to bootstrap the catalog, ensuring all system tables are present and
    /// accessible.
    pub async fn try_bootstrap(&self) -> Result<()> {
        unimplemented!()
    }

    pub async fn begin(&self) -> Result<TransactionalCatalog> {
        unimplemented!()
    }
}

pub struct TransactionalCatalog {}

impl TransactionalCatalog {
    pub async fn get_schema_catalog(&self, name: &str) -> Result<Option<SchemaCatalog<'_>>> {
        unimplemented!()
    }
}

pub struct SchemaCatalog<'a> {
    id: SchemaId,
    catalog: &'a TransactionalCatalog,
}

impl<'a> SchemaCatalog<'a> {
    pub async fn get_table_catalog(&self, name: &str) -> Result<Option<TableCatalog<'a>>> {
        unimplemented!()
    }

    pub async fn create_table(&mut self, name: String, schema: Schema) -> Result<()> {
        unimplemented!()
    }
}

pub struct TableCatalog<'a> {
    id: TableId,
    catalog: &'a SchemaCatalog<'a>,
}

impl<'a> TableCatalog<'a> {
    pub async fn table_partitions(&self) -> Result<Vec<PartitionId>> {
        Ok(vec![0]) // All tables have a single partition for now.
    }

    pub async fn arrow_schema(&self) -> Result<Schema> {
        unimplemented!()
    }

    // TODO: Get a "reference" to the table.
}
