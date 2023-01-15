use crate::errors::Result;
use crate::types::catalog::{CatalogEntry, CatalogState, SchemaEntry};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The session local catalog.
pub struct SessionCatalog {
    /// The state retrieved from a remote Metastore.
    state: Arc<CatalogState>,
    /// Map schema names to their ids.
    schema_names: HashMap<String, u32>,
    /// Map object names (tables, views, etc) to their ids.
    object_names: HashMap<ObjectNameKey, u32>,
}

impl SessionCatalog {
    /// Maybe swap the session local catalog with a new catalog state.
    ///
    /// This will only swap if the new state contains a catalog state with a
    /// newer version.
    // TODO: This should also _not_ swap if inside a transaction.
    pub fn maybe_swap(&mut self, new_state: &Arc<CatalogState>) {
        if self.state.version < new_state.version {
            debug!(old = %self.state.version, new = %new_state.version, "swapping catalog");
            self.state = new_state.clone();
            self.rebuild_name_maps();
        }
    }

    /// Resolve a schema by name.
    pub fn resolve_schema(&self, name: &str) -> Option<&SchemaEntry> {
        // This function will panic if certain invariants aren't held:
        //
        // - If the name is found in the name map, then the associated id must
        //   exist in the catalog state.
        // - The catalog entry type that the id points to must be a schema.

        let id = self.schema_names.get(name)?;
        let ent = self.state.entries.get(id).expect(&format!(
            "schema name points to invalid id; name: {}, id: {}",
            name, id
        ));

        match ent {
            CatalogEntry::Schema(s) => Some(s),
            _ => panic!(
                "entry type not schema; name: {}, id: {}, type: {:?}",
                name,
                id,
                ent.entry_type(),
            ),
        }
    }

    /// Resolve an entry by schema name and object name.
    ///
    /// Note that this will never return a schema entry.
    pub fn resolve_entry(&self, schema: &str, name: &str) -> Option<&CatalogEntry> {
        unimplemented!()
    }

    fn rebuild_name_maps(&mut self) {
        self.schema_names.clear();
        self.object_names.clear();

        for (id, ent) in &self.state.entries {
            let name = ent.get_meta().name.clone();

            if ent.is_schema() {
                self.schema_names.insert(name, *id);
            } else {
                let key = ObjectNameKey {
                    schema_id: ent.get_meta().parent,
                    object_name: name,
                };
                self.object_names.insert(key, *id);
            }
        }
    }
}

/// Identify database objects by name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ObjectNameKey {
    schema_id: u32,
    object_name: String,
}
