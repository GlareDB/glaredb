use std::fmt::Debug;

use futures::future::BoxFuture;
use rayexec_error::{RayexecError, Result};

use crate::database::catalog_entry::TableEntry;
use crate::database::memory_catalog::MemoryCatalog;

pub trait CatalogStorage: Debug + Sync + Send {
    fn initial_load(&self, catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>>;

    fn persist(&self, catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>>;

    fn load_schemas<'a>(
        &'a self,
        _catalog: &'a MemoryCatalog,
    ) -> Result<BoxFuture<'a, Result<()>>> {
        Err(RayexecError::new("load_schemas not implemented"))
    }

    fn load_table(&self, schema: &str, name: &str) -> BoxFuture<'_, Result<Option<TableEntry>>>;
}
