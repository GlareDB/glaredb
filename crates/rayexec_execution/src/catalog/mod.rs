pub mod context;
pub mod create;
pub mod database;
pub mod drop;
pub mod entry;
pub mod memory;
pub mod system;

use std::fmt::Debug;
use std::sync::Arc;

use create::{
    CreateAggregateFunctionInfo,
    CreateScalarFunctionInfo,
    CreateSchemaInfo,
    CreateTableFunctionInfo,
    CreateTableInfo,
    CreateViewInfo,
};
use drop::DropInfo;
use entry::{CatalogEntry, CatalogEntryType};
use rayexec_error::{RayexecError, Result};

use crate::execution::operators::PlannedOperator;
use crate::storage::storage_manager::{StorageManager, StorageTableId};

pub trait Catalog: Debug + Sync + Send {
    type Schema: Schema;

    /// Create a schema in the catalog.
    fn create_schema(&self, create: &CreateSchemaInfo) -> Result<Arc<Self::Schema>>;

    /// Get a schema in the catalog.
    ///
    /// Returns Ok(None) if a schema with the given name doesn't exist.
    fn get_schema(&self, name: &str) -> Result<Option<Arc<Self::Schema>>>;

    /// Get a schema, returning an error if it doesn't exist.
    fn require_get_schema(&self, name: &str) -> Result<Arc<Self::Schema>> {
        self.get_schema(name)?
            .ok_or_else(|| RayexecError::new(format!("Missing schema '{name}'")))
    }

    /// Drop an entry in the catalog.
    fn drop_entry(&self, drop: &DropInfo) -> Result<()>;

    fn plan_create_view(
        self: &Arc<Self>,
        schema: &str,
        create: CreateViewInfo,
    ) -> Result<PlannedOperator>;

    fn plan_create_table(
        self: &Arc<Self>,
        storage: &Arc<StorageManager>,
        schema: &str,
        create: CreateTableInfo,
    ) -> Result<PlannedOperator>;

    fn plan_insert(
        self: &Arc<Self>,
        storage: &Arc<StorageManager>,
        entry: Arc<CatalogEntry>,
    ) -> Result<PlannedOperator>;

    fn plan_create_schema(self: &Arc<Self>, create: CreateSchemaInfo) -> Result<PlannedOperator>;
}

pub trait Schema: Debug + Sync + Send {
    /// Create a table in the schema.
    // TODO: Storage id should be opaque.
    fn create_table(
        &self,
        create: &CreateTableInfo,
        storage_id: StorageTableId,
    ) -> Result<Arc<CatalogEntry>>;

    /// Create a view in the schema.
    fn create_view(&self, create: &CreateViewInfo) -> Result<Arc<CatalogEntry>>;

    /// Create a scalar function in the schema.
    fn create_scalar_function(
        &self,
        create: &CreateScalarFunctionInfo,
    ) -> Result<Arc<CatalogEntry>>;

    /// Create an aggregate function in the schema.
    fn create_aggregate_function(
        &self,
        create: &CreateAggregateFunctionInfo,
    ) -> Result<Arc<CatalogEntry>>;

    /// Create a table function in the schema.
    fn create_table_function(&self, create: &CreateTableFunctionInfo) -> Result<Arc<CatalogEntry>>;

    /// Get a table or view in the schema.
    fn get_table_or_view(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>>;

    fn require_get_table(&self, name: &str) -> Result<Arc<CatalogEntry>> {
        let ent = self
            .get_table_or_view(name)?
            .ok_or_else(|| RayexecError::new(format!("Missing table '{name}'")))?;
        if ent.entry_type() != CatalogEntryType::Table {
            return Err(RayexecError::new(format!("'{name}' is not a table")));
        }
        Ok(ent)
    }

    /// Get a table function in the schema.
    fn get_table_function(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>>;

    /// Get a scalar or aggregate function from the schema.
    fn get_function(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>>;

    /// Get a scalar function from the schema.
    fn get_scalar_function(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>>;

    /// Get an aggregate function from the schema.
    fn get_aggregate_function(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>>;

    /// Find a similar entry in the catalog that's of the given entry type.
    fn find_similar_entry(
        &self,
        entry_types: &[CatalogEntryType],
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>>;
}
