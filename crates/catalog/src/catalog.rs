use crate::errors::{CatalogError, Result};
use crate::system::{
    builtin_types::BUILTIN_TYPES_TABLE_NAME, schemas::SCHEMAS_TABLE_NAME, SystemSchema,
    SYSTEM_SCHEMA_ID, SYSTEM_SCHEMA_NAME,
};
use access::runtime::AccessRuntime;
use access::strategy::SinglePartitionStrategy;
use access::table::PartitionedTable;
use catalog_types::keys::{PartitionId, SchemaId, TableId, TableKey};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use std::future::Future;
use std::sync::Arc;

/// The top-level catalog.
pub struct DatabaseCatalog {
    system: Arc<SystemSchema>,
    runtime: Arc<AccessRuntime>,
}

impl DatabaseCatalog {
    /// Open up a catalog to some persistent storage.
    pub async fn open(runtime: Arc<AccessRuntime>) -> Result<DatabaseCatalog> {
        // TODO: Check system tables exist, bootstrap.
        let system = Arc::new(SystemSchema::bootstrap()?);
        Ok(DatabaseCatalog { system, runtime })
    }

    pub async fn begin(&self) -> Result<TransactionalCatalog> {
        Ok(TransactionalCatalog {
            system: self.system.clone(),
            runtime: self.runtime.clone(),
        })
    }
}

pub struct TransactionalCatalog {
    system: Arc<SystemSchema>,
    runtime: Arc<AccessRuntime>,
}

impl TransactionalCatalog {
    pub async fn get_schema_catalog(&self, name: &str) -> Result<Option<SchemaCatalog<'_>>> {
        let schema = match name {
            SYSTEM_SCHEMA_NAME => SchemaCatalog {
                id: SYSTEM_SCHEMA_ID,
                catalog: self,
                runtime: &self.runtime,
            },
            _ => unimplemented!(),
        };

        Ok(Some(schema))
    }

    pub async fn new_schema(&self, name: &str) -> Result<()> {
        let schemas = self
            .system
            .get_system_table(SCHEMAS_TABLE_NAME)
            .ok_or(CatalogError::MissingSystemTable(
                SCHEMAS_TABLE_NAME.to_string(),
            ))?
            .get_table(self.runtime.clone())
            .get_partitioned_table()?;

        // Get sequence for schema values.
        // Insert into "schemas" table.

        unimplemented!()
    }
}

pub struct SchemaCatalog<'a> {
    id: SchemaId,
    catalog: &'a TransactionalCatalog,
    runtime: &'a Arc<AccessRuntime>,
}

impl<'a> SchemaCatalog<'a> {
    pub async fn get_table_catalog(&self, _name: &str) -> Result<Option<TableCatalog<'a>>> {
        unimplemented!()
    }

    pub async fn create_table(&mut self, _name: String, _schema: Schema) -> Result<()> {
        unimplemented!()
    }
}

pub struct TableCatalog<'a> {
    id: TableId,
    catalog: &'a SchemaCatalog<'a>,
    runtime: &'a Arc<AccessRuntime>,
}

impl<'a> TableCatalog<'a> {
    pub async fn table_partitions(&self) -> Result<Vec<PartitionId>> {
        Ok(vec![0]) // All tables have a single partition for now.
    }

    pub async fn arrow_schema(&self) -> Result<SchemaRef> {
        unimplemented!()
    }

    pub async fn table(&self) -> Result<PartitionedTable> {
        let key = TableKey {
            schema_id: self.catalog.id,
            table_id: self.id,
        };
        let schema = self.arrow_schema().await?;

        Ok(PartitionedTable::new(
            key,
            Box::new(SinglePartitionStrategy),
            self.runtime.clone(),
            schema,
        ))
    }
}
