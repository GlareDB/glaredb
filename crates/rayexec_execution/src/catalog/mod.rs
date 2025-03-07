pub mod context;
pub mod create;
pub mod drop;
pub mod entry;
pub mod memory;
pub mod system;
pub mod transaction;

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
use rayexec_error::Result;

pub trait Catalog: Debug + Sync + Send {
    type CatalogTx: CatalogTx;
    type Schema: Schema<CatalogTx = Self::CatalogTx>;

    /// Create a schema in the catalog.
    fn create_schema(
        &self,
        tx: &Self::CatalogTx,
        create: &CreateSchemaInfo,
    ) -> Result<Self::Schema>;

    /// Get a schema in the catalog.
    fn get_schema(&self, tx: &Self::CatalogTx, name: &str) -> Result<Option<Self::Schema>>;

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

pub trait CatalogTx: Debug {}
