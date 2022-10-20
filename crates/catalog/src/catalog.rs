use crate::errors::Result;
use catalog_types::keys::{PartitionId, SchemaId, TableId};
use datafusion::arrow::datatypes::Schema;

pub struct DatabaseCatalog {}

impl DatabaseCatalog {
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
pub struct SnapshotDatabaseCatalog {}

impl SnapshotDatabaseCatalog {
    pub async fn get_schema(&self, name: &str) -> Result<Option<SchemaCatalog>> {
        unimplemented!()
    }

    pub async fn drop_schema(&self, name: &str) -> Result<()> {
        unimplemented!()
    }
}

pub struct SchemaCatalog {
    id: SchemaId,
}

impl SchemaCatalog {
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

pub struct TableMeta {
    id: TableId,
}

impl TableMeta {
    /// Get a list of partitions that make up this table.
    // TODO: This will also provide sparse indexes.
    pub async fn table_partitions(&self) -> Result<Vec<PartitionId>> {
        unimplemented!()
    }

    pub async fn arrow_schema(&self) -> Result<Schema> {
        unimplemented!()
    }
}
