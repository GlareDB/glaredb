use rayexec_error::Result;
use std::fmt::Debug;

use crate::functions::{aggregate::GenericAggregateFunction, scalar::GenericScalarFunction};

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
    fn get_table_entry(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<TableEntry>>;

    fn get_scalar_fn(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn GenericScalarFunction>>>;

    fn get_aggregate_fn(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn GenericAggregateFunction>>>;

    fn data_table(
        &self,
        tx: &CatalogTx,
        schema: &str,
        ent: &TableEntry,
    ) -> Result<Box<dyn DataTable>>;

    fn catalog_modifier(&self, tx: &CatalogTx) -> Result<Box<dyn CatalogModifier>>;
}

/// Implementation of Catalog over a shared catalog (e.g. the global system
/// catalog that cannot be changed).
impl Catalog for &dyn Catalog {
    fn get_table_entry(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<TableEntry>> {
        (*self).get_table_entry(tx, schema, name)
    }

    fn get_scalar_fn(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn GenericScalarFunction>>> {
        (*self).get_scalar_fn(tx, schema, name)
    }

    fn get_aggregate_fn(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn GenericAggregateFunction>>> {
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
