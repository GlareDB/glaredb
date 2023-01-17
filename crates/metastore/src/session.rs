use crate::proto::service::metastore_service_client::MetastoreServiceClient;
use crate::types::catalog::{CatalogEntry, CatalogState, EntryType, SchemaEntry};
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
    /// Create a new session catalog with an initial state.
    pub fn new(state: Arc<CatalogState>) -> SessionCatalog {
        let mut catalog = SessionCatalog {
            state,
            schema_names: HashMap::new(),
            schema_objects: HashMap::new(),
        };
        catalog.rebuild_name_maps();
        catalog
    }

    /// Get the version of this catalog state.
    pub fn version(&self) -> u64 {
        self.state.version
    }

    /// Swap the underlying state of the catalog.
    pub fn swap_state(&mut self, new_state: Arc<CatalogState>) {
        self.state = new_state;
        self.rebuild_name_maps();
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

    /// Iterate over all entries in this catalog.
    ///
    /// All non-schema entries will also include an entry pointing to its parent
    /// schema.
    pub fn iter_entries(&self) -> impl Iterator<Item = NamespacedCatalogEntry> {
        self.state.entries.iter().map(|(oid, entry)| {
            let schema_entry = if !entry.is_schema() {
                Some(self.state.entries.get(&entry.get_meta().parent).unwrap()) // Bug if it doesn't exist.
            } else {
                None
            };
            NamespacedCatalogEntry {
                oid: *oid,
                schema_entry,
                entry,
            }
        })
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

/// An entry that's possibly namespaces by a schema.
#[derive(Debug)]
pub struct NamespacedCatalogEntry<'a> {
    /// The OID of the entry.
    pub oid: u32,
    /// The parent schema for this entry. This will be `None` when the entry
    /// itself is a schema.
    pub schema_entry: Option<&'a CatalogEntry>,
    /// The entry.
    pub entry: &'a CatalogEntry,
}

impl NamespacedCatalogEntry<'_> {
    /// Get the entry type for this entry.
    pub fn entry_type(&self) -> EntryType {
        self.entry.get_meta().entry_type
    }
}
