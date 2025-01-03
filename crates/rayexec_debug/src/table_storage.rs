use std::sync::Arc;

use futures::future::BoxFuture;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::arrays::batch::Batch2;
use rayexec_execution::arrays::field::Field;
use rayexec_execution::database::catalog_entry::CatalogEntry;
use rayexec_execution::execution::operators::sink::PartitionSink;
use rayexec_execution::storage::table_storage::{
    DataTable,
    DataTableScan,
    ProjectedScan,
    Projections,
    TableStorage,
};

// Much of the debug table implementation was copied from the memory table
// implemenation in the execution crate. I opted to copy it in since the memory
// table stuff might end up getting more complex to support additional table
// features like conflicts and updates.
//
// The debug data source only needs the simple implementation (for now, may
// change).

#[derive(Debug)]
pub struct TablePreload {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Field>,
    pub data: Batch2,
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
                "Missing physical debug table for entry: {ent:?}. Cannot get data table",
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
                    "Duplicate physical debug table for entry: {:?}",
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
                    "Missing physical debug for entry: {key:?}. Cannot drop table.",
                )));
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct DebugDataTable {
    data: Arc<Mutex<Vec<Batch2>>>,
}

impl DataTable for DebugDataTable {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>> {
        let mut scans: Vec<_> = (0..num_partitions)
            .map(|_| DebugDataTableScan { data: Vec::new() })
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
            .map(|scan| Box::new(ProjectedScan::new(scan, projections.clone())) as Box<_>)
            .collect())
    }

    fn insert(&self, input_partitions: usize) -> Result<Vec<Box<dyn PartitionSink>>> {
        let inserts: Vec<_> = (0..input_partitions)
            .map(|_| {
                Box::new(DebugDataTableInsert {
                    collected: Vec::new(),
                    data: self.data.clone(),
                }) as _
            })
            .collect();

        Ok(inserts)
    }
}

#[derive(Debug)]
pub struct DebugDataTableScan {
    data: Vec<Batch2>,
}

impl DataTableScan for DebugDataTableScan {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch2>>> {
        Box::pin(async { Ok(self.data.pop()) })
    }
}

#[derive(Debug)]
pub struct DebugDataTableInsert {
    collected: Vec<Batch2>,
    data: Arc<Mutex<Vec<Batch2>>>,
}

impl PartitionSink for DebugDataTableInsert {
    fn push(&mut self, batch: Batch2) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            self.collected.push(batch);
            Ok(())
        })
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            let mut data = self.data.lock();
            data.append(&mut self.collected);
            Ok(())
        })
    }
}
