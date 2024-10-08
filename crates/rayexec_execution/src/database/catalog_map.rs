use std::sync::Arc;

use rayexec_error::{RayexecError, Result};
use scc::ebr::Guard;

use super::catalog::CatalogTx;
use super::catalog_entry::CatalogEntry;

/// Maps a name to some catalog entry.
#[derive(Debug, Default)]
pub struct CatalogMap {
    entries: scc::HashIndex<String, Arc<CatalogEntry>>,
}

impl CatalogMap {
    pub fn create_entry(&self, _tx: &CatalogTx, entry: CatalogEntry) -> Result<()> {
        match self.entries.entry(entry.name.clone()) {
            scc::hash_index::Entry::Occupied(ent) => Err(RayexecError::new(format!(
                "Duplicate entry name '{}'",
                ent.name
            ))),
            scc::hash_index::Entry::Vacant(ent) => {
                ent.insert_entry(Arc::new(entry));
                Ok(())
            }
        }
    }

    pub fn drop_entry(&self, _tx: &CatalogTx, entry: &CatalogEntry) -> Result<()> {
        if !self.entries.remove(&entry.name) {
            return Err(RayexecError::new(format!("Missing entry '{}'", entry.name)));
        }
        Ok(())
    }

    pub fn get_entry(&self, _tx: &CatalogTx, name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        let guard = Guard::new();
        let ent = self.entries.peek(name, &guard).cloned();
        Ok(ent)
    }

    pub fn for_each_entry<F>(&self, _tx: &CatalogTx, func: &mut F) -> Result<()>
    where
        F: FnMut(&String, &Arc<CatalogEntry>) -> Result<()>,
    {
        let guard = Guard::new();
        for (name, ent) in self.entries.iter(&guard) {
            func(name, ent)?;
        }
        Ok(())
    }
}
