//! Module for handling the catalog for a single database.
use crate::errors::{MetastoreError, Result};
use crate::types::catalog::{
    CatalogEntry, CatalogState, EntryMeta, EntryType, SchemaEntry, ViewEntry,
};
use crate::types::service::Mutation;
use parking_lot::{Mutex, MutexGuard};
use std::collections::HashMap;
use uuid::Uuid;

/// Special id indicating that schemas have no parents.
const SCHEMA_PARENT_ID: u32 = 0;

/// Catalog for a single database.
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
                oid_counter: 0,
                entries: HashMap::new(),
                schema_names: HashMap::new(),
                schema_objects: HashMap::new(),
            }),
        })
    }

    /// Get the current state of the catalog.
    pub async fn get_state(&self) -> Result<CatalogState> {
        let state = self.state.lock();
        Ok(self.serializable_state(state))
    }

    /// Try to mutate the catalog.
    ///
    /// Errors if the provided version doesn't match the version of the current
    /// catalog.
    ///
    /// On success, a full copy of the updated catalog state will be returned.
    // TODO: All or none.
    pub async fn try_mutate(&self, version: u64, mutations: Vec<Mutation>) -> Result<CatalogState> {
        // TODO: Try lock?
        // TODO: Lease lock storage.
        let mut state = self.state.lock();
        if state.version != version {
            return Err(MetastoreError::VersionMismtatch {
                have: version,
                need: state.version,
            });
        }

        // TODO: Validate mutations.

        state.mutate(mutations)?;

        let updated = self.serializable_state(state);

        // TODO: Write to storage.
        // TODO: Rollback on failed flush.

        Ok(updated)
    }

    /// Return the serializable state of the catalog at this version.
    fn serializable_state(&self, guard: MutexGuard<State>) -> CatalogState {
        CatalogState {
            db_id: self.db_id,
            version: guard.version,
            entries: guard.entries.clone(),
        }
    }
}

/// Inner state of the catalog.
struct State {
    /// Version incremented on every update.
    version: u64,
    /// Next OID to use.
    oid_counter: u32,
    /// All entries in the catalog.
    entries: HashMap<u32, CatalogEntry>,
    /// Map schema names to their ids.
    schema_names: HashMap<String, u32>,
    /// Map schema IDs to objects in the schema.
    schema_objects: HashMap<u32, SchemaObjects>,
}

impl State {
    /// Get the next oid to use for a catalog entry.
    fn next_oid(&mut self) -> u32 {
        let oid = self.oid_counter;
        self.oid_counter += 1;
        oid
    }

    /// Execute mutations against the state.
    fn mutate(&mut self, mutations: Vec<Mutation>) -> Result<()> {
        // We don't care if this overflows. When comparing versions, we just
        // care about if they're different. We can always assume that if a a
        // session's catalog does not match metastore's catalog version, then
        // the session's catalog is out of date.
        (self.version, _) = self.version.overflowing_add(1);

        for mutation in mutations {
            match mutation {
                Mutation::DropSchema(drop_schema) => {
                    // TODO: Dependency checking.
                    let schema_id = self
                        .schema_names
                        .remove(&drop_schema.name)
                        .ok_or_else(|| MetastoreError::MissingNamedSchema(drop_schema.name))?;
                    _ = schema_id
                }
                Mutation::DropObject(drop_object) => {
                    // TODO: Dependency checking.
                    let schema_id =
                        self.schema_names.get(&drop_object.schema).ok_or_else(|| {
                            MetastoreError::MissingNamedSchema(drop_object.schema.clone())
                        })?;
                    let objs = self.schema_objects.get_mut(schema_id).unwrap(); // Bug if doesn't exist.
                    let ent_id = objs.objects.remove(&drop_object.name).ok_or_else(|| {
                        MetastoreError::MissingNamedObject {
                            schema: drop_object.schema,
                            name: drop_object.name,
                        }
                    })?;
                    let _ = self.entries.remove(&ent_id).unwrap(); // Bug if doesn't exist.
                }
                Mutation::CreateSchema(create_schema) => {
                    // TODO: If not exists.
                    if self.schema_names.contains_key(&create_schema.name) {
                        return Err(MetastoreError::DuplicateName(create_schema.name));
                    }

                    // Create new entry
                    let oid = self.next_oid();
                    let ent = SchemaEntry {
                        meta: EntryMeta {
                            entry_type: EntryType::Schema,
                            id: oid,
                            parent: SCHEMA_PARENT_ID,
                            name: create_schema.name.clone(),
                        },
                    };
                    self.entries.insert(oid, CatalogEntry::Schema(ent));

                    // Add to name map
                    self.schema_names.insert(create_schema.name, oid);
                }
                Mutation::CreateView(create_view) => {
                    // TODO: If not exists.

                    let schema_id = self
                        .schema_names
                        .get(&create_view.schema)
                        .cloned()
                        .ok_or_else(|| {
                            MetastoreError::MissingNamedSchema(create_view.schema.clone())
                        })?;

                    // Create new entry
                    let oid = self.next_oid();
                    let ent = ViewEntry {
                        meta: EntryMeta {
                            entry_type: EntryType::View,
                            id: oid,
                            parent: schema_id,
                            name: create_view.name.clone(),
                        },
                        sql: create_view.sql,
                    };

                    // Insert new entry for schema. Checks if there exists an
                    // object with the same name.
                    let objs = self.schema_objects.entry(schema_id).or_default();
                    if objs.objects.contains_key(&create_view.name) {
                        return Err(MetastoreError::DuplicateName(create_view.name));
                    }
                    objs.objects.insert(create_view.name, oid);

                    // Insert into catalog.
                    self.entries.insert(oid, CatalogEntry::View(ent));
                }
                _ => unimplemented!(),
            }
        }

        Ok(())
    }
}

/// Holds names to object ids for a single schema.
#[derive(Debug, Default)]
struct SchemaObjects {
    /// Maps names to ids in this schema.
    objects: HashMap<String, u32>,
}
