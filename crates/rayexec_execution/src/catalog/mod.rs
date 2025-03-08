pub mod context;
pub mod create;
pub mod database;
pub mod drop;
pub mod entry;
pub mod memory;
pub mod storage_manager;
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

pub trait CatalogTx: Debug {}

pub trait Catalog: Debug + Sync + Send {
    type CatalogTx: CatalogTx;
    type Schema: Schema<CatalogTx = Self::CatalogTx>;

    /// Create a schema in the catalog.
    fn create_schema(
        &self,
        tx: &Self::CatalogTx,
        create: &CreateSchemaInfo,
    ) -> Result<Arc<Self::Schema>>;

    /// Get a schema in the catalog.
    ///
    /// Returns Ok(None) if a schema with the given name doesn't exist.
    fn get_schema(&self, tx: &Self::CatalogTx, name: &str) -> Result<Option<Arc<Self::Schema>>>;

    /// Get a schema, returning an error if it doesn't exist.
    fn require_get_schema(&self, tx: &Self::CatalogTx, name: &str) -> Result<Arc<Self::Schema>> {
        self.get_schema(tx, name)?
            .ok_or_else(|| RayexecError::new("Missing schema '{name}'"))
    }

    /// Drop an entry in the catalog.
    fn drop_entry(&self, tx: &Self::CatalogTx, drop: &DropInfo) -> Result<()>;
}

pub trait Schema: Debug + Sync + Send {
    type CatalogTx: CatalogTx;

    /// Create a table in the schema.
    fn create_table(
        &self,
        tx: &Self::CatalogTx,
        create: &CreateTableInfo,
    ) -> Result<Arc<CatalogEntry>>;

    /// Create a view in the schema.
    fn create_view(
        &self,
        tx: &Self::CatalogTx,
        create: &CreateViewInfo,
    ) -> Result<Arc<CatalogEntry>>;

    /// Create a scalar function in the schema.
    fn create_scalar_function(
        &self,
        tx: &Self::CatalogTx,
        create: &CreateScalarFunctionInfo,
    ) -> Result<Arc<CatalogEntry>>;

    /// Create an aggregate function in the schema.
    fn create_aggregate_function(
        &self,
        tx: &Self::CatalogTx,
        create: &CreateAggregateFunctionInfo,
    ) -> Result<Arc<CatalogEntry>>;

    /// Create a table function in the schema.
    fn create_table_function(
        &self,
        tx: &Self::CatalogTx,
        create: &CreateTableFunctionInfo,
    ) -> Result<Arc<CatalogEntry>>;

    /// Get a table or view in the schema.
    fn get_table_or_view(
        &self,
        tx: &Self::CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>>;

    /// Get a table function in the schema.
    fn get_table_function(
        &self,
        tx: &Self::CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>>;

    /// Get a scalar or aggregate function from the schema.
    fn get_function(&self, tx: &Self::CatalogTx, name: &str) -> Result<Option<Arc<CatalogEntry>>>;

    /// Get a scalar function from the schema.
    fn get_scalar_function(
        &self,
        tx: &Self::CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>>;

    /// Get an aggregate function from the schema.
    fn get_aggregate_function(
        &self,
        tx: &Self::CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>>;

    /// Find a similar entry in the catalog that's of the given entry type.
    fn find_similar_entry(
        &self,
        tx: &Self::CatalogTx,
        entry_types: &[CatalogEntryType],
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>>;
}

// TODO: The Arc<Self> is a bit weird, but it's mostly for the
// `plan_create_schema` to be able to clone the outer catalog.
pub trait CatalogPlanner: Catalog {
    fn plan_create_view(
        self: &Arc<Self>,
        tx: &Self::CatalogTx,
        schema: &str,
        create: CreateViewInfo,
    ) -> Result<PlannedOperator>;

    fn plan_create_schema(
        self: &Arc<Self>,
        tx: &Self::CatalogTx,
        create: CreateSchemaInfo,
    ) -> Result<PlannedOperator>;
}
