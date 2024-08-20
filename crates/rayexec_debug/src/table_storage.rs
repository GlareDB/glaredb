use std::sync::Arc;

use futures::future::BoxFuture;
use parking_lot::Mutex;
use rayexec_bullet::{batch::Batch, field::Field};
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::catalog_entry::CatalogEntry,
    storage::table_storage::{DataTable, DataTableScan, TableStorage},
};

#[derive(Debug)]
pub struct TablePreload {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Field>,
    pub data: Batch,
}

#[derive(Debug, Default)]
pub struct DebugTableStorage {
    tables: scc::HashIndex<TableKey, DebugDataTable>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TableKey {
    schema: String,
    name: String,
}

impl DebugTableStorage {
    pub fn new_with_tables(preload_tables: &[TablePreload]) -> Self {
        let tables = scc::HashIndex::new();

        for table in preload_tables {
            tables
                .insert(
                    TableKey {
                        schema: table.schema.clone(),
                        name: table.name.clone(),
                    },
                    DebugDataTable {
                        data: Arc::new(Mutex::new(vec![table.data.clone()])),
                    },
                )
                .expect("table to not already exist");
        }

        DebugTableStorage { tables }
    }
}

impl TableStorage for DebugTableStorage {
    fn data_table(&self, schema: &str, ent: &CatalogEntry) -> Result<Box<dyn DataTable>> {
        let key = TableKey {
            schema: schema.to_string(),
            name: ent.name.clone(),
        };

        let table = self.tables.get(&key).ok_or_else(|| {
            RayexecError::new(format!(
                "Missing physical memory table for entry: {ent:?}. Cannot get data table",
            ))
        })?;

        Ok(Box::new(table.get().clone()))
    }

    fn create_physical_table(
        &self,
        schema: &str,
        ent: &CatalogEntry,
    ) -> BoxFuture<'_, Result<Box<dyn DataTable>>> {
        let key = TableKey {
            schema: schema.to_string(),
            name: ent.name.clone(),
        };

        Box::pin(async {
            match self.tables.entry(key) {
                scc::hash_index::Entry::Occupied(ent) => Err(RayexecError::new(format!(
                    "Duplicate physical table for entry: {:?}",
                    ent.key(),
                ))),
                scc::hash_index::Entry::Vacant(hash_ent) => {
                    let table = DebugDataTable::default();
                    hash_ent.insert_entry(table.clone());
                    Ok(Box::new(table) as _)
                }
            }
        })
    }

    fn drop_physical_table(&self, schema: &str, ent: &CatalogEntry) -> BoxFuture<'_, Result<()>> {
        let key = TableKey {
            schema: schema.to_string(),
            name: ent.name.clone(),
        };

        Box::pin(async move {
            if !self.tables.remove(&key) {
                return Err(RayexecError::new(format!(
                    "Missing physical memory table for entry: {key:?}. Cannot drop table.",
                )));
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct DebugDataTable {
    data: Arc<Mutex<Vec<Batch>>>,
}

impl DataTable for DebugDataTable {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let mut scans: Vec<_> = (0..num_partitions)
            .map(|_| MemoryDataTableScan { data: Vec::new() })
            .collect();

        let data = {
            let data = self.data.lock();
            data.clone()
        };

        for (idx, batch) in data.into_iter().enumerate() {
            scans[idx % num_partitions].data.push(batch);
        }

        Ok(scans
            .into_iter()
            .map(|scan| Box::new(scan) as Box<_>)
            .collect())
    }
}

#[derive(Debug)]
pub struct MemoryDataTableScan {
    data: Vec<Batch>,
}

impl DataTableScan for MemoryDataTableScan {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async { Ok(self.data.pop()) })
    }
}
