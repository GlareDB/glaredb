pub mod discard;
pub mod table_storage;

use std::collections::HashMap;
use std::sync::Arc;

use discard::DiscardCopyToFunction;
use futures::future::BoxFuture;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::arrays::scalar::ScalarValue;
use rayexec_execution::database::catalog_entry::TableEntry;
use rayexec_execution::database::memory_catalog::MemoryCatalog;
use rayexec_execution::datasource::{DataSource, DataSourceConnection, DataSourceCopyTo};
use rayexec_execution::functions::table::TableFunction;
use rayexec_execution::storage::catalog_storage::CatalogStorage;
use table_storage::{DebugTableStorage, TablePreload};

// TODO: Some weirdness with interaction between catalog storage and table
// storage.

#[derive(Debug)]
pub struct DebugDataSourceOptions {
    /// Tables to preload the source with.
    pub preloads: Vec<TablePreload>,

    /// Options we expect to receive on connect, errors if we receive options
    /// that don't exactly match these.
    pub expected_options: HashMap<String, ScalarValue>,

    /// Format string to use for using the discard COPY TO implementation.
    pub discard_format: String,
}

#[derive(Debug)]
pub struct DebugDataSource {
    catalog_storage: Arc<DebugCatalogStorage>,
    table_storage: Arc<DebugTableStorage>,
    opts: DebugDataSourceOptions,
}

impl DebugDataSource {
    pub fn new(opts: DebugDataSourceOptions) -> Self {
        let mut catalog_storage = DebugCatalogStorage {
            tables: HashMap::new(),
        };

        for table in &opts.preloads {
            catalog_storage.tables.insert(
                [table.schema.clone(), table.name.clone()],
                TableEntry {
                    columns: table.columns.clone(),
                },
            );
        }

        let catalog_storage = Arc::new(catalog_storage);
        let table_storage = Arc::new(DebugTableStorage::new_with_tables(&opts.preloads));

        DebugDataSource {
            catalog_storage,
            table_storage,
            opts,
        }
    }
}

impl DataSource for DebugDataSource {
    fn connect(
        &self,
        options: HashMap<String, ScalarValue>,
    ) -> BoxFuture<'_, Result<DataSourceConnection>> {
        Box::pin(async move {
            if options != self.opts.expected_options {
                return Err(RayexecError::new(
                    "Provided options do not match expected options",
                ));
            }

            Ok(DataSourceConnection {
                catalog_storage: Some(self.catalog_storage.clone()),
                table_storage: self.table_storage.clone(),
            })
        })
    }

    fn initialize_copy_to_functions(&self) -> Vec<DataSourceCopyTo> {
        vec![DataSourceCopyTo {
            format: self.opts.discard_format.clone(),
            copy_to: Box::new(DiscardCopyToFunction),
        }]
    }

    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        Vec::new()
    }
}

#[derive(Debug)]
struct DebugCatalogStorage {
    tables: HashMap<[String; 2], TableEntry>,
}

impl CatalogStorage for DebugCatalogStorage {
    fn initial_load(&self, _catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn persist(&self, _catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn load_table(&self, schema: &str, name: &str) -> BoxFuture<'_, Result<Option<TableEntry>>> {
        let key = [schema.to_string(), name.to_string()];
        Box::pin(async move { Ok(self.tables.get(&key).cloned()) })
    }
}
