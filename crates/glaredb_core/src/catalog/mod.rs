pub mod context;
pub mod create;
pub mod database;
pub mod drop;
pub mod entry;
pub mod memory;
pub mod profile;
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
use futures::Stream;
use glaredb_error::{DbError, Result};

use crate::execution::operators::PlannedOperator;
use crate::execution::planner::OperatorIdGen;
use crate::storage::storage_manager::{StorageManager, StorageTableId};

// TODO: This will probably undergo the same "manual dynamic dispatch" as many
// of the other things in this code base.
//
// We'll need to make a distinction between what can actually be implemented by
// external catalogs (e.g. fetching tables) vs what can't and require
// "in-memory" implementations (e.g. storing functions).
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
            .ok_or_else(|| DbError::new(format!("Missing schema '{name}'")))
    }

    /// Drop an entry in the catalog.
    ///
    /// Returns the dropped entry, if it exists.
    fn drop_entry(&self, drop: &DropInfo) -> Result<Option<Arc<CatalogEntry>>>;

    fn plan_create_view(
        self: &Arc<Self>,
        id_gen: &mut OperatorIdGen,
        schema: &str,
        create: CreateViewInfo,
    ) -> Result<PlannedOperator>;

    fn plan_create_table(
        self: &Arc<Self>,
        storage: &Arc<StorageManager>,
        id_gen: &mut OperatorIdGen,
        schema: &str,
        create: CreateTableInfo,
    ) -> Result<PlannedOperator>;

    fn plan_create_table_as(
        self: &Arc<Self>,
        storage: &Arc<StorageManager>,
        id_gen: &mut OperatorIdGen,
        schema: &str,
        create: CreateTableInfo,
    ) -> Result<PlannedOperator>;

    fn plan_insert(
        self: &Arc<Self>,
        storage: &Arc<StorageManager>,
        id_gen: &mut OperatorIdGen,
        entry: Arc<CatalogEntry>,
    ) -> Result<PlannedOperator>;

    fn plan_create_schema(
        self: &Arc<Self>,
        id_gen: &mut OperatorIdGen,
        create: CreateSchemaInfo,
    ) -> Result<PlannedOperator>;

    fn plan_drop(
        self: &Arc<Self>,
        storage: &Arc<StorageManager>,
        id_gen: &mut OperatorIdGen,
        drop: DropInfo,
    ) -> Result<PlannedOperator>;

    fn list_schemas(
        self: &Arc<Self>,
    ) -> impl Stream<Item = Result<Vec<Arc<Self::Schema>>>> + Sync + Send + 'static;
}

pub trait Schema: Debug + Sync + Send {
    fn as_entry(&self) -> Arc<CatalogEntry>;

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
            .ok_or_else(|| DbError::new(format!("Missing table '{name}'")))?;
        if ent.entry_type() != CatalogEntryType::Table {
            return Err(DbError::new(format!("'{name}' is not a table")));
        }
        Ok(ent)
    }

    /// Get a table function in the schema.
    fn get_table_function(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>>;

    /// Get a table function by trying to infer which one to use from the
    /// provided path.
    fn get_inferred_table_function(&self, path: &str) -> Result<Option<Arc<CatalogEntry>>>;

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

    /// List all entries in the schema.
    fn list_entries(
        self: &Arc<Self>,
    ) -> impl Stream<Item = Result<Vec<Arc<CatalogEntry>>>> + Sync + Send + 'static;

    fn list_tables(
        self: &Arc<Self>,
    ) -> impl Stream<Item = Result<Vec<Arc<CatalogEntry>>>> + Sync + Send + 'static;
}
