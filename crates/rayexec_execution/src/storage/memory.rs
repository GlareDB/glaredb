use std::sync::Arc;

use futures::future::BoxFuture;
use parking_lot::Mutex;
use crate::arrays::batch::Batch;
use rayexec_error::{RayexecError, Result};

use super::table_storage::{DataTable, DataTableScan, ProjectedScan, Projections, TableStorage};
use crate::database::catalog_entry::CatalogEntry;
use crate::execution::computed_batch::ComputedBatches;
use crate::execution::operators::sink::PartitionSink;
use crate::execution::operators::util::resizer::{BatchResizer, DEFAULT_TARGET_BATCH_SIZE};

#[derive(Debug, Default)]
pub struct MemoryTableStorage {
    tables: scc::HashIndex<TableKey, MemoryDataTable>,
}

// Temporary, we'd want to key by oid or something to handle table renames
// easily.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TableKey {
    schema: String,
    name: String,
}

impl TableStorage for MemoryTableStorage {
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
                    let table = MemoryDataTable::default();
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
pub struct MemoryDataTable {
    data: Arc<Mutex<Vec<Batch>>>,
}

impl DataTable for MemoryDataTable {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>> {
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
            .map(|scan| Box::new(ProjectedScan::new(scan, projections.clone())) as Box<_>)
            .collect())
    }

    fn insert(&self, input_partitions: usize) -> Result<Vec<Box<dyn PartitionSink>>> {
        let inserts: Vec<_> = (0..input_partitions)
            .map(|_| {
                Box::new(MemoryDataTableInsert {
                    resizer: BatchResizer::new(DEFAULT_TARGET_BATCH_SIZE),
                    collected: Vec::new(),
                    data: self.data.clone(),
                }) as _
            })
            .collect();

        Ok(inserts)
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

#[derive(Debug)]
pub struct MemoryDataTableInsert {
    resizer: BatchResizer, // TODO: Need to replace.
    collected: Vec<ComputedBatches>,
    data: Arc<Mutex<Vec<Batch>>>,
}

impl PartitionSink for MemoryDataTableInsert {
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            let batches = self.resizer.try_push(batch)?;
            if batches.is_empty() {
                return Ok(());
            }
            self.collected.push(batches);
            Ok(())
        })
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            let batches = self.resizer.flush_remaining()?;
            self.collected.push(batches);

            let mut data = self.data.lock();
            for mut computed in self.collected.drain(..) {
                while let Some(batch) = computed.try_pop_front()? {
                    data.push(batch);
                }
            }

            Ok(())
        })
    }
}
