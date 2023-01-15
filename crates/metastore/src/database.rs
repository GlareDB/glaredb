//! Module for handling the catalog for a single database.
use crate::errors::{MetastoreError, Result};
use crate::types::catalog::CatalogEntry;
use parking_lot::Mutex;
use std::collections::HashMap;
use uuid::Uuid;

pub struct DatabaseCatalog {
    db_id: Uuid,
    state: Mutex<State>, // TODO: Replace with storage.
}

impl DatabaseCatalog {
    /// Open the catalog for a database.
    pub async fn open(id: Uuid) -> Result<DatabaseCatalog> {
        // TODO: Storage
        Ok(DatabaseCatalog {
            db_id: id,
            state: Mutex::new(State {
                version: 0,
                entries: HashMap::new(),
                name_map: HashMap::new(),
                oid_counter: 0,
            }),
        })
    }
}

/// Inner state of the catalog.
struct State {
    /// Version incremented on every update.
    version: u64,
    entries: HashMap<u32, CatalogEntry>,
    /// Map entry names to IDs.
    name_map: HashMap<String, u32>,
    oid_counter: u32,
}

impl State {
    /// Get the next oid to use for a catalog entry.
    fn next_oid(&mut self) -> u32 {
        let oid = self.oid_counter;
        self.oid_counter += 1;
        oid
    }
}
