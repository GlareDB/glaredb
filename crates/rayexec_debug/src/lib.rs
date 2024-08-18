use futures::future::BoxFuture;
use rayexec_bullet::{field::Field, scalar::OwnedScalarValue};
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::{catalog_entry::TableEntry, memory_catalog::MemoryCatalog},
    datasource::{DataSource, DataSourceConnection},
    functions::table::TableFunction,
    storage::{catalog_storage::CatalogStorage, memory::MemoryTableStorage},
};
use std::{collections::HashMap, sync::Arc};

// TODO: Preload with data.
#[derive(Debug)]
pub struct TablePreload {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Field>,
}

// TODO: Some weirdness with interaction between catalog storage and table
// storage.

#[derive(Debug)]
pub struct DebugDataSource {
    catalog_storage: Arc<DebugCatalogStorage>,
    table_storage: Arc<MemoryTableStorage>,
    /// Options we expect to receive, errors if we receive options that don't
    /// exactly match these.
    expected_options: HashMap<String, OwnedScalarValue>,
}

impl DebugDataSource {
    pub fn new(
        preload: impl IntoIterator<Item = TablePreload>,
        expected_options: impl Into<HashMap<String, OwnedScalarValue>>,
    ) -> Self {
        let mut catalog_storage = DebugCatalogStorage {
            tables: HashMap::new(),
        };

        for table in preload {
            catalog_storage.tables.insert(
                [table.schema, table.name],
                TableEntry {
                    columns: table.columns,
                },
            );
        }

        let catalog_storage = Arc::new(catalog_storage);
        let table_storage = Arc::new(MemoryTableStorage::default());

        DebugDataSource {
            catalog_storage,
            table_storage,
            expected_options: expected_options.into(),
        }
    }
}

impl DataSource for DebugDataSource {
    fn connect(
        &self,
        options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'_, Result<DataSourceConnection>> {
        Box::pin(async move {
            if options != self.expected_options {
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
