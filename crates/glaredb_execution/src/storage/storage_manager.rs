use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{self, AtomicUsize};

use glaredb_error::{DbError, Result};

use super::datatable::DataTable;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageTableId(usize);

#[derive(Debug)]
pub struct StorageManager {
    next_id: AtomicUsize,
    tables: scc::HashMap<StorageTableId, Arc<DataTable>>,
}

impl StorageManager {
    pub fn empty() -> Self {
        StorageManager {
            next_id: AtomicUsize::new(0),
            tables: scc::HashMap::new(),
        }
    }

    pub fn insert_table(&self, table: Arc<DataTable>) -> Result<StorageTableId> {
        let id = self.next_id.fetch_add(1, atomic::Ordering::Relaxed);
        let id = StorageTableId(id);

        // Failing here shouldn't happen.
        self.tables
            .insert(id, table)
            .map_err(|_| DbError::new(format!("Table with id already exists: {id:?}")))?;

        Ok(id)
    }

    pub fn drop_table(&self, id: StorageTableId) -> Result<()> {
        if self.tables.remove(&id).is_none() {
            return Err(DbError::new(format!("Missing table for id: {id:?}")));
        }
        Ok(())
    }

    pub fn get_table(&self, id: StorageTableId) -> Result<Arc<DataTable>> {
        let table = self
            .tables
            .get(&id)
            .ok_or_else(|| DbError::new(format!("Missing table for id: {id:?}")))?;
        Ok(table.get().clone())
    }
}
