use crate::errors::Result;
use access::table::PartitionedTable;
use catalog_types::keys::{PartitionId, SchemaId, TableId};
use datafusion::arrow::datatypes::Schema;
use std::sync::Arc;

#[derive(Default)] // TODO: Remove.
pub struct DatabaseCatalog {}

impl DatabaseCatalog {
    // TODO: Provide access runtime.
    pub async fn connect_storage(&self) -> Result<DatabaseCatalog> {
        unimplemented!()
    }

    /// Try to bootstrap the catalog, ensuring system tables are present.
    pub async fn try_bootstrap(&self) -> Result<()> {
        unimplemented!()
    }

    pub async fn open(&self) -> Result<SnapshotDatabaseCatalog> {
        unimplemented!()
    }
}

/// A point in time catalog.
// TODO: Eventually be transactional.
#[derive(Default)] // TODO: Remove
pub struct SnapshotDatabaseCatalog {
    db: Arc<DatabaseCatalog>,
}

impl SnapshotDatabaseCatalog {
    pub async fn get_schema(&self, name: &str) -> Result<Option<SchemaCatalog>> {
        unimplemented!()
    }

    pub async fn drop_schema(&self, name: &str) -> Result<()> {
        unimplemented!()
    }
}

pub struct SchemaCatalog<'a> {
    id: SchemaId,
    snapshot: &'a SnapshotDatabaseCatalog,
}

impl<'a> SchemaCatalog<'a> {
    pub async fn create_table(&self, name: String, schema: Schema) -> Result<()> {
        unimplemented!()
    }

    pub async fn get_table(&self, name: &str) -> Result<Option<TableMeta>> {
        unimplemented!()
    }

    pub async fn drop_table(&self, name: &str) -> Result<()> {
        unimplemented!()
    }
}

pub struct TableMeta<'a> {
    id: TableId,
    schema: &'a SchemaCatalog<'a>,
}

impl<'a> TableMeta<'a> {
    /// Get a list of partitions that make up this table.
    // TODO: This will also provide sparse indexes.
    pub async fn table_partitions(&self) -> Result<Vec<PartitionId>> {
        unimplemented!()
    }

    pub async fn arrow_schema(&self) -> Result<Schema> {
        unimplemented!()
    }

    /// Get a reference to the underlying partitioned table.
    pub async fn table(&self) -> Result<PartitionedTable> {
        unimplemented!()
    }
}
