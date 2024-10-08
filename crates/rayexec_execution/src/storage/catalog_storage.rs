use std::fmt::Debug;

use futures::future::BoxFuture;
use rayexec_error::Result;

use crate::database::catalog_entry::TableEntry;
use crate::database::memory_catalog::MemoryCatalog;

pub trait CatalogStorage: Debug + Sync + Send {
    fn initial_load(&self, catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>>;

    fn persist(&self, catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>>;

    fn load_table(&self, schema: &str, name: &str) -> BoxFuture<'_, Result<Option<TableEntry>>>;
}
