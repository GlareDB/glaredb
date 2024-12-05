use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::database::catalog::CatalogTx;
use rayexec_execution::database::catalog_entry::TableEntry;
use rayexec_execution::database::create::{CreateSchemaInfo, OnConflict};
use rayexec_execution::database::memory_catalog::MemoryCatalog;
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::catalog_storage::CatalogStorage;

use crate::connection::UnityCatalogConnection;

#[derive(Debug, Clone)]
pub struct UnityCatalog<R: Runtime> {
    connection: UnityCatalogConnection<R>,
}

impl<R: Runtime> UnityCatalog<R> {
    pub fn new(connection: UnityCatalogConnection<R>) -> Self {
        UnityCatalog { connection }
    }

    async fn load_schemas_inner(&self, catalog: &MemoryCatalog) -> Result<()> {
        let mut stream = Box::pin(self.connection.list_schemas()?.into_stream());

        let tx = &CatalogTx::new(); // TODO

        while let Some(resp) = stream.try_next().await? {
            for schema in resp.schemas {
                catalog.create_schema(
                    tx,
                    &CreateSchemaInfo {
                        name: schema.name,
                        on_conflict: OnConflict::Ignore,
                    },
                )?;
            }
        }

        Ok(())
    }
}

impl<R: Runtime> CatalogStorage for UnityCatalog<R> {
    fn initial_load(&self, _catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { Ok(()) })
    }

    fn persist(&self, catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>> {
        unimplemented!()
    }

    fn load_schemas<'a>(&'a self, catalog: &'a MemoryCatalog) -> Result<BoxFuture<'_, Result<()>>> {
        Ok(Box::pin(async { self.load_schemas_inner(catalog).await }))
    }

    fn load_table(&self, schema: &str, name: &str) -> BoxFuture<'_, Result<Option<TableEntry>>> {
        unimplemented!()
    }
}
