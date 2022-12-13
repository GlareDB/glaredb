use crate::catalog::Context;
use crate::errors::{internal, CatalogError, Result};
use crate::transaction::Timestamp;
use parking_lot::RwLock;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::sync::Arc;

pub enum EntryInsert<T> {
    /// Entry was successfully inserted.
    Inserted,
    /// Entry wasn't inserted.
    NotInserted(T),
}

impl<T> EntryInsert<T> {
    pub fn expect_inserted(&self) -> Result<()> {
        match &self {
            EntryInsert => Ok(()),
            Self::NotInserted(_) => Err(internal!("expected entry to be inserted")),
        }
    }
}

pub enum EntryDrop {
    /// Entry was successfully dropped.
    Dropped,
    /// Entry wasn't found.
    NotFound,
}

/// Holds entries of one type for the catalog.
pub struct EntrySet<T> {
    inner: RwLock<Inner<T>>,
}

struct Inner<T> {
    /// Mapping of name to entries.
    mapping: BTreeMap<String, EntryMapping>,
    /// Catalog entries.
    entries: BTreeMap<usize, Arc<T>>,
    /// Index for the current entry.
    curr_entry_idx: usize,
}

impl<T> EntrySet<T> {
    pub fn create_entry<C: Context>(
        &self,
        ctx: &C,
        name: String,
        entry: T,
    ) -> Result<EntryInsert<T>> {
        let mut inner = self.inner.write();

        // TODO: Make this transactional by storing the old as child on the new
        // entry.
        //
        // Detect write-write conflicts.

        let idx = inner.curr_entry_idx;
        inner.curr_entry_idx += 1;

        match inner.mapping.entry(name) {
            btree_map::Entry::Occupied(ent) => {
                // TODO: This leaks. Doesn't cleanup the old entry (and it's no
                // longer mapped).
                let new_mapping = EntryMapping {
                    idx,
                    ts: Timestamp::default(),
                    deleted: false,
                    child: None,
                };
                let ent = ent.into_mut();
                *ent = new_mapping;
            }
            btree_map::Entry::Vacant(ent) => {
                ent.insert(EntryMapping {
                    idx,
                    ts: Timestamp::default(),
                    deleted: false,
                    child: None,
                });
            }
        }

        inner.entries.insert(idx, Arc::new(entry));

        Ok(EntryInsert::Inserted)
    }

    pub fn get_entry<C: Context>(&self, ctx: &C, name: &str) -> Option<Arc<T>> {
        let inner = self.inner.read();

        // TODO: Transactional lookup.

        let mapping = inner.mapping.get(name)?;
        if mapping.deleted {
            return None;
        }
        let ent = inner.entries.get(&mapping.idx).unwrap(); // Programmer error if this panics.

        Some(ent.clone())
    }

    pub fn drop_entry<C: Context>(&self, ctx: &C, name: &str) -> Result<EntryDrop> {
        let mut inner = self.inner.write();

        // TODO: Transactional lookup.

        let idx = match inner.mapping.get_mut(name) {
            Some(ent) => {
                ent.deleted = true;
                Some(ent.idx)
            }
            None => None,
        };

        match idx {
            Some(idx) => {
                inner.entries.remove(&idx);
                Ok(EntryDrop::Dropped)
            }
            None => Ok(EntryDrop::NotFound),
        }
    }
}

/// A string to index mapping for an entry.
#[derive(Default)]
struct EntryMapping {
    idx: usize,
    ts: Timestamp,
    deleted: bool,
    child: Option<Box<EntryMapping>>,
}
