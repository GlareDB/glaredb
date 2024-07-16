use futures::future::BoxFuture;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;

use crate::functions::{
    aggregate::AggregateFunction, scalar::ScalarFunction, table::TableFunction,
};

use super::{ddl::CatalogModifier, entry::TableEntry, table::DataTable};

#[derive(Debug, Default)]
pub struct CatalogTx {}

impl CatalogTx {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Interface for accessing data.
///
/// It's expected that each data source implements its own version of the
/// catalog (and consequently a schema implementation). If a data source doens't
/// support a given operation (e.g. create schema for our bigquery data source),
/// an appropriate error should be returned.
pub trait Catalog: Debug + Sync + Send {
    /// Get a table entry.
    ///
    /// This is async as we may be working with remote catalog and we don't want
    /// to preload all table entries into memory.
    fn get_table_entry(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> BoxFuture<Result<Option<TableEntry>>>;

    /// Get a scalar function.
    ///
    /// All scalar functions for a catalog should be loaded into memory to allow
    /// for synchronous resolution. Function deserialization looks up functions
    /// in the catalog so this this needs to be sync.
    fn get_scalar_fn(
        &self,
        _tx: &CatalogTx,
        _schema: &str,
        _name: &str,
    ) -> Result<Option<Box<dyn ScalarFunction>>> {
        Err(RayexecError::new("Cannot get scalar function from catalog"))
    }

    /// Get an aggregate function.
    ///
    /// See `get_scalar_fn` for why this is sync.
    fn get_aggregate_fn(
        &self,
        _tx: &CatalogTx,
        _schema: &str,
        _name: &str,
    ) -> Result<Option<Box<dyn AggregateFunction>>> {
        Err(RayexecError::new(
            "Cannot get aggregate function from catalog",
        ))
    }

    /// Get a table function.
    ///
    /// See `get_scalar_fn` for why this is sync.
    fn get_table_fn(
        &self,
        _tx: &CatalogTx,
        _schema: &str,
        _name: &str,
    ) -> Result<Option<Box<dyn TableFunction>>> {
        Err(RayexecError::new("Cannot get table function from catalog"))
    }

    /// Create a datatable for interacting with a given table.
    fn data_table(
        &self,
        tx: &CatalogTx,
        schema: &str,
        ent: &TableEntry,
    ) -> Result<Box<dyn DataTable>>;

    /// Get a catalog modifier for the catalog.
    ///
    /// Defaults to erroring.
    fn catalog_modifier(&self, _tx: &CatalogTx) -> Result<Box<dyn CatalogModifier>> {
        Err(RayexecError::new("Cannot modify catalog"))
    }
}

/// Implementation of Catalog over a shared catalog (e.g. the global system
/// catalog that cannot be changed).
impl Catalog for &dyn Catalog {
    fn get_table_entry(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> BoxFuture<Result<Option<TableEntry>>> {
        (*self).get_table_entry(tx, schema, name)
    }

    fn get_scalar_fn(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn ScalarFunction>>> {
        (*self).get_scalar_fn(tx, schema, name)
    }

    fn get_aggregate_fn(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn AggregateFunction>>> {
        (*self).get_aggregate_fn(tx, schema, name)
    }

    fn data_table(
        &self,
        tx: &CatalogTx,
        schema: &str,
        ent: &TableEntry,
    ) -> Result<Box<dyn DataTable>> {
        (*self).data_table(tx, schema, ent)
    }

    fn catalog_modifier(&self, tx: &CatalogTx) -> Result<Box<dyn CatalogModifier>> {
        (*self).catalog_modifier(tx)
    }
}
