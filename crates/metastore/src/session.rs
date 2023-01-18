use crate::types::catalog::{CatalogEntry, CatalogState, SchemaEntry};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The session local catalog.
///
/// This catalog should be stored on the "client" side and periodically updated
/// from the remote state provided by metastore.
pub struct SessionCatalog {
    /// The state retrieved from a remote Metastore.
    state: Arc<CatalogState>,
    /// Map schema names to their ids.
    schema_names: HashMap<String, u32>,
    /// Map schema IDs to objects in the schema.
    schema_objects: HashMap<u32, SchemaObjects>,
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
        let ent = self
            .state
            .entries
            .get(id)
            .expect("schema name points to invalid id");

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
        let schema_id = self.schema_names.get(schema)?;
        let obj = self.schema_objects.get(schema_id)?;
        let obj_id = obj.objects.get(name)?;

        let ent = self
            .state
            .entries
            .get(obj_id)
            .expect("object name points to invalid id");
        assert!(!ent.is_schema());

        Some(ent)
    }

    fn rebuild_name_maps(&mut self) {
        self.schema_names.clear();
        self.schema_objects.clear();

        for (id, ent) in &self.state.entries {
            let name = ent.get_meta().name.clone();

            if ent.is_schema() {
                self.schema_names.insert(name, *id);
            } else {
                let schema_id = ent.get_meta().parent;
                let ent = self.schema_objects.entry(schema_id).or_default();
                ent.objects.insert(name, *id);
            }
        }
    }
}

/// Holds names to object ids for a single schema.
#[derive(Debug, Default)]
struct SchemaObjects {
    /// Maps names to ids in this schema.
    objects: HashMap<String, u32>,
}
