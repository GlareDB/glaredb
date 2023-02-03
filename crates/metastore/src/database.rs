//! Module for handling the catalog for a single database.
use crate::builtins::{BuiltinSchema, BuiltinTable, BuiltinView, FIRST_NON_SCHEMA_ID};
use crate::errors::{MetastoreError, Result};
use crate::storage::persist::Storage;
use crate::types::catalog::{
    CatalogEntry, CatalogState, ConnectionEntry, EntryMeta, EntryType, ExternalTableEntry,
    SchemaEntry, TableEntry, ViewEntry,
};
use crate::types::service::Mutation;
use crate::types::storage::PersistedCatalog;
use once_cell::sync::Lazy;
use pgrepr::oid::FIRST_AVAILABLE_ID;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};
use tracing::debug;
use uuid::Uuid;

/// Special id indicating that schemas have no parents.
const SCHEMA_PARENT_ID: u32 = 0;

/// A global builtin catalog. This is meant to be cloned for every database
/// catalog.
static BUILTIN_CATALOG: Lazy<BuiltinCatalog> = Lazy::new(|| BuiltinCatalog::new().unwrap());

/// Catalog for a single database.
pub struct DatabaseCatalog {
    db_id: Uuid,

    /// Reference to underlying persistant storage.
    storage: Arc<Storage>,

    /// A cached catalog state for a single database.
    cached: Mutex<State>,

    /// Whether or not to force a full reload of the catalog from object
    /// storage.
    // TODO: Remove when `State` has transactional semantics.
    require_full_load: AtomicBool,
}

impl DatabaseCatalog {
    /// Open the catalog for a database.
    pub async fn open(db_id: Uuid, storage: Arc<Storage>) -> Result<DatabaseCatalog> {
        // Always initialize, idempotent.
        storage.initialize(db_id).await?;

        let persisted = storage.read_catalog(db_id).await?;
        let state = State::from_persisted(persisted)?;

        Ok(DatabaseCatalog {
            db_id,
            storage,
            cached: Mutex::new(state),
            require_full_load: AtomicBool::new(false),
        })
    }

    /// Get the current state of the catalog.
    pub async fn get_state(&self) -> Result<CatalogState> {
        // TODO: Reduce locking.
        self.load_latest().await?;

        let state = self.cached.lock().await;
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
        debug!(db_id = %self.db_id, %version, ?mutations, "mutating catalog");

        // TODO: Reduce locking.
        self.load_latest().await?;

        let mut state = self.cached.lock().await;
        if state.version != version {
            return Err(MetastoreError::VersionMismtatch {
                have: version,
                need: state.version,
            });
        }

        // TODO: Validate mutations.

        // State's version number updated, but we still need to use the old
        // version number when making a request to storage.
        let old_version = version;

        // TODO: Rollback on failed mutate.
        //
        // Currently don't have guarantees about what the state looks like on
        // failed mutates. Force a reload.
        //
        // Fixed with <https://github.com/GlareDB/glaredb/issues/547>.
        if let Err(e) = state.mutate(mutations) {
            self.require_full_load.store(true, Ordering::Relaxed);
            return Err(e);
        }

        let persist = state.to_persisted();
        let updated = self.serializable_state(state);

        // TODO: Rollback on failed flush.
        //
        // Currently this will keep a copy of a catalog in cache without having
        // it persisted. We'll want `State` to be somewhat transactional,
        // allowing for a single writer and multiple readers.
        //
        // As a stop gap, we can use a `require_full_load` flag which will force
        // us to a do a full reload on the catalog. This does require that we
        // hold the lock for the cached state across the write to storage, which
        // has significant performance implications, but does guarantee we'll
        // never serve an invalid catalog.
        if let Err(e) = self
            .storage
            .write_catalog(self.db_id, old_version, persist)
            .await
        {
            self.require_full_load.store(true, Ordering::Relaxed);
            return Err(e.into());
        }

        Ok(updated)
    }

    /// Return the serializable state of the catalog at this version.
    fn serializable_state(&self, guard: MutexGuard<State>) -> CatalogState {
        CatalogState {
            version: guard.version,
            entries: guard.entries.clone(),
        }
    }

    /// Load the latest state from object storage.
    async fn load_latest(&self) -> Result<()> {
        let current_version = {
            let cached = self.cached.lock().await;
            cached.version
        };

        let latest_version = self.storage.latest_version(&self.db_id).await?;

        // We must have the latest version and _not_ have a full load required.
        if current_version == latest_version && !self.require_full_load.load(Ordering::Relaxed) {
            return Ok(());
        }
        debug!(db_id = %self.db_id, %current_version, %latest_version, "loading latest catalog for database");

        // Otherwise rebuild the state from object storage...

        let persisted = self.storage.read_catalog(self.db_id).await?;
        let state = State::from_persisted(persisted)?;

        let mut cached = self.cached.lock().await;
        if cached.version != current_version {
            // Concurrent call to this function, we can assume that we have the
            // updated state then.
            debug!("concurrent update to cached state");
            return Ok(());
        }
        *cached = state;

        // Reset full load flag.
        self.require_full_load.store(false, Ordering::Relaxed);

        Ok(())
    }
}

/// Inner state of the catalog.
#[derive(Debug)]
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
    /// Create a new state from a persisted catalog.
    ///
    /// The state will be combined with a predefinend builtin catalog objects.
    ///
    /// This will build the schema names and objects maps.
    fn from_persisted(persisted: PersistedCatalog) -> Result<State> {
        let mut state = State {
            version: persisted.version,
            oid_counter: persisted.oid_counter,
            entries: persisted.entries,
            schema_names: HashMap::new(),
            schema_objects: HashMap::new(),
        };

        // Sanity check to ensure we didn't accidentally persist builtin
        // objects.
        for (oid, ent) in &state.entries {
            if *oid < FIRST_AVAILABLE_ID || ent.get_meta().builtin {
                return Err(MetastoreError::BuiltinObjectPersisted(
                    ent.get_meta().clone(),
                ));
            }
        }

        // Extend with builtin objects.
        let builtin = BUILTIN_CATALOG.clone();
        state.entries.extend(builtin.entries);
        state.schema_names.extend(builtin.schema_names);
        state.schema_objects.extend(builtin.schema_objects);

        // Rebuild name maps for user objects.
        for (oid, entry) in state
            .entries
            .iter()
            .filter(|(_, ent)| !ent.get_meta().builtin)
        {
            if entry.is_schema() {
                if entry.get_meta().parent != SCHEMA_PARENT_ID {
                    return Err(MetastoreError::SchemaHasNonZeroParent {
                        schema: *oid,
                        parent: entry.get_meta().parent,
                    });
                }

                state
                    .schema_names
                    .insert(entry.get_meta().name.clone(), *oid);
            } else {
                // Only schemas may have a parent with ID 0. All other objects
                // must have non-zero parent IDs.
                if entry.get_meta().parent == SCHEMA_PARENT_ID {
                    return Err(MetastoreError::ObjectHasInvalidParentId {
                        object: *oid,
                        parent: entry.get_meta().parent,
                    });
                }

                let schema_id = entry.get_meta().parent;

                let objects = state.schema_objects.entry(schema_id).or_default();
                let existing = objects.objects.insert(entry.get_meta().name.clone(), *oid);
                if let Some(existing) = existing {
                    return Err(MetastoreError::DuplicateNameFoundDuringLoad {
                        name: entry.get_meta().name.clone(),
                        schema: schema_id,
                        first: *oid,
                        second: existing,
                    });
                }
            }
        }

        Ok(state)
    }

    /// Create a persisted catalog containing only user objects.
    ///
    /// Builtins are added to the catalog when converting from a persisted
    /// catalog.
    fn to_persisted(&self) -> PersistedCatalog {
        PersistedCatalog {
            version: self.version,
            entries: self
                .entries
                .clone()
                .into_iter()
                .filter(|(_, ent)| !ent.get_meta().builtin)
                .collect(),
            oid_counter: self.oid_counter,
        }
    }

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
                        .ok_or(MetastoreError::MissingNamedSchema(drop_schema.name))?;
                    _ = schema_id
                }
                Mutation::DropObject(drop_object) => {
                    // TODO: Dependency checking.
                    let schema_id = self.get_schema_id(&drop_object.name)?;

                    let objs = self.schema_objects.get_mut(&schema_id).unwrap(); // Bug if doesn't exist.
                    let ent_id = objs.objects.remove(&drop_object.name).ok_or(
                        MetastoreError::MissingNamedObject {
                            schema: drop_object.schema,
                            name: drop_object.name,
                        },
                    )?;
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
                            builtin: false,
                        },
                    };
                    self.entries.insert(oid, CatalogEntry::Schema(ent));

                    // Add to name map
                    self.schema_names.insert(create_schema.name, oid);
                }
                Mutation::CreateView(create_view) => {
                    // TODO: If not exists.

                    let schema_id = self.get_schema_id(&create_view.schema)?;

                    // Create new entry
                    let oid = self.next_oid();
                    let ent = ViewEntry {
                        meta: EntryMeta {
                            entry_type: EntryType::View,
                            id: oid,
                            parent: schema_id,
                            name: create_view.name.clone(),
                            builtin: false,
                        },
                        sql: create_view.sql,
                    };

                    self.try_insert_entry_for_schema(CatalogEntry::View(ent), schema_id, oid)?;
                }
                Mutation::CreateConnection(create_conn) => {
                    // TODO: If not exists.

                    let schema_id = self.get_schema_id(&create_conn.schema)?;

                    // Create new entry.
                    let oid = self.next_oid();
                    let ent = ConnectionEntry {
                        meta: EntryMeta {
                            entry_type: EntryType::Connection,
                            id: oid,
                            parent: schema_id,
                            name: create_conn.name.clone(),
                            builtin: false,
                        },
                        options: create_conn.options,
                    };

                    self.try_insert_entry_for_schema(
                        CatalogEntry::Connection(ent),
                        schema_id,
                        oid,
                    )?;
                }
                Mutation::CreateExternalTable(create_ext) => {
                    // TODO: If not exists.

                    let schema_id = self.get_schema_id(&create_ext.schema)?;

                    if !self.entries.contains_key(&create_ext.connection_id) {
                        return Err(MetastoreError::MissingEntry(create_ext.connection_id));
                    }

                    // Create new entry.
                    let oid = self.next_oid();
                    let ent = ExternalTableEntry {
                        meta: EntryMeta {
                            entry_type: EntryType::ExternalTable,
                            id: oid,
                            parent: schema_id,
                            name: create_ext.name.clone(),
                            builtin: false,
                        },
                        connection_id: create_ext.connection_id,
                        options: create_ext.options,
                    };

                    self.try_insert_entry_for_schema(
                        CatalogEntry::ExternalTable(ent),
                        schema_id,
                        oid,
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Try to insert an entry for a schema.
    ///
    /// Errors if there already exists an entry with the same name in the
    /// schema. Consequentially this means all entries in a schema share the
    /// same namespace even for different entry types. E.g. a connection cannot
    /// have the same name as a table.
    fn try_insert_entry_for_schema(
        &mut self,
        ent: CatalogEntry,
        schema_id: u32,
        oid: u32,
    ) -> Result<()> {
        // Insert new entry for schema. Checks if there exists an
        // object with the same name.
        let objs = self.schema_objects.entry(schema_id).or_default();

        if objs.objects.contains_key(&ent.get_meta().name) {
            return Err(MetastoreError::DuplicateName(ent.get_meta().name.clone()));
        }
        objs.objects.insert(ent.get_meta().name.clone(), oid);

        // Insert connection.
        self.entries.insert(oid, ent);
        Ok(())
    }

    fn get_schema_id(&self, name: &str) -> Result<u32> {
        self.schema_names
            .get(name)
            .cloned()
            .ok_or_else(|| MetastoreError::MissingNamedSchema(name.to_string()))
    }
}

/// Holds names to object ids for a single schema.
#[derive(Debug, Default, Clone)]
struct SchemaObjects {
    /// Maps names to ids in this schema.
    objects: HashMap<String, u32>,
}

/// Catalog with builtin objects. Used during database catalog initialization.
#[derive(Clone)]
struct BuiltinCatalog {
    /// All entries in the catalog.
    entries: HashMap<u32, CatalogEntry>,
    /// Map schema names to their ids.
    schema_names: HashMap<String, u32>,
    /// Map schema IDs to objects in the schema.
    schema_objects: HashMap<u32, SchemaObjects>,
}

impl BuiltinCatalog {
    /// Create a new builtin catalog.
    ///
    /// It is a programmer error if this fails to build.
    fn new() -> Result<BuiltinCatalog> {
        let mut entries = HashMap::new();
        let mut schema_names = HashMap::new();
        let mut schema_objects = HashMap::new();

        for schema in BuiltinSchema::builtins() {
            schema_names.insert(schema.name.to_string(), schema.oid);
            schema_objects.insert(schema.oid, SchemaObjects::default());
            entries.insert(
                schema.oid,
                CatalogEntry::Schema(SchemaEntry {
                    meta: EntryMeta {
                        entry_type: EntryType::Schema,
                        id: schema.oid,
                        parent: SCHEMA_PARENT_ID,
                        name: schema.name.to_string(),
                        builtin: true,
                    },
                }),
            );
        }

        // All the below items don't have stable ids.
        let mut oid = FIRST_NON_SCHEMA_ID;

        for table in BuiltinTable::builtins() {
            let schema_id = schema_names
                .get(table.schema)
                .ok_or_else(|| MetastoreError::MissingNamedSchema(table.schema.to_string()))?;
            entries.insert(
                oid,
                CatalogEntry::Table(TableEntry {
                    meta: EntryMeta {
                        entry_type: EntryType::Table,
                        id: oid,
                        parent: *schema_id,
                        name: table.name.to_string(),
                        builtin: true,
                    },
                    columns: table.columns.clone(),
                }),
            );
            schema_objects
                .get_mut(schema_id)
                .unwrap()
                .objects
                .insert(table.name.to_string(), oid);

            oid += 1;
        }

        for view in BuiltinView::builtins() {
            let schema_id = schema_names
                .get(view.schema)
                .ok_or_else(|| MetastoreError::MissingNamedSchema(view.schema.to_string()))?;
            entries.insert(
                oid,
                CatalogEntry::View(ViewEntry {
                    meta: EntryMeta {
                        entry_type: EntryType::View,
                        id: oid,
                        parent: *schema_id,
                        name: view.name.to_string(),
                        builtin: true,
                    },
                    sql: view.sql.to_string(),
                }),
            );
            schema_objects
                .get_mut(schema_id)
                .unwrap()
                .objects
                .insert(view.name.to_string(), oid);

            oid += 1;
        }

        Ok(BuiltinCatalog {
            entries,
            schema_names,
            schema_objects,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::persist::Storage;
    use crate::types::catalog::{ConnectionOptions, ConnectionOptionsDebug};
    use crate::types::service::{CreateConnection, CreateSchema, CreateView, DropSchema};
    use object_store::memory::InMemory;
    use std::collections::HashSet;

    async fn new_catalog() -> DatabaseCatalog {
        logutil::init_test();
        let store = Arc::new(InMemory::new());
        let storage = Arc::new(Storage::new(Uuid::new_v4(), store));
        DatabaseCatalog::open(Uuid::new_v4(), storage)
            .await
            .unwrap()
    }

    async fn version(db: &DatabaseCatalog) -> u64 {
        db.get_state().await.unwrap().version
    }

    #[test]
    fn builtin_catalog_builds() {
        BuiltinCatalog::new().unwrap();
    }

    #[tokio::test]
    async fn drop_missing_schema() {
        let db = new_catalog().await;

        db.try_mutate(
            version(&db).await,
            vec![Mutation::DropSchema(DropSchema {
                name: "yoshi".to_string(),
            })],
        )
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn multiple_entries() {
        let db = new_catalog().await;

        db.try_mutate(
            version(&db).await,
            vec![Mutation::CreateSchema(CreateSchema {
                name: "numbers".to_string(),
            })],
        )
        .await
        .unwrap();

        let mutations: Vec<_> = (0..10)
            .map(|i| {
                Mutation::CreateView(CreateView {
                    schema: "numbers".to_string(),
                    name: i.to_string(),
                    sql: format!("select {i}"),
                })
            })
            .collect();

        db.try_mutate(version(&db).await, mutations).await.unwrap();

        let state = db.get_state().await.unwrap();

        let mut found = HashSet::new();
        for (_, ent) in state.entries {
            if !ent.is_schema() {
                found.insert(ent.get_meta().name.clone());
            }
        }

        for i in 0..10 {
            assert!(found.contains(i.to_string().as_str()));
        }
    }

    #[tokio::test]
    async fn duplicate_schema_names() {
        let db = new_catalog().await;

        // First should pass.
        db.try_mutate(
            version(&db).await,
            vec![Mutation::CreateSchema(CreateSchema {
                name: "mario".to_string(),
            })],
        )
        .await
        .unwrap();

        // Duplicate should fail.
        db.try_mutate(
            version(&db).await,
            vec![Mutation::CreateSchema(CreateSchema {
                name: "mario".to_string(),
            })],
        )
        .await
        .unwrap_err();

        // Drop schema.
        db.try_mutate(
            version(&db).await,
            vec![Mutation::DropSchema(DropSchema {
                name: "mario".to_string(),
            })],
        )
        .await
        .unwrap();

        // Re-adding schema should pass.
        db.try_mutate(
            version(&db).await,
            vec![Mutation::CreateSchema(CreateSchema {
                name: "mario".to_string(),
            })],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn duplicate_entry_names() {
        let db = new_catalog().await;

        // Add schema.
        db.try_mutate(
            version(&db).await,
            vec![Mutation::CreateSchema(CreateSchema {
                name: "luigi".to_string(),
            })],
        )
        .await
        .unwrap();

        // Add view.
        db.try_mutate(
            version(&db).await,
            vec![Mutation::CreateView(CreateView {
                schema: "luigi".to_string(),
                name: "peach".to_string(),
                sql: "select 1".to_string(),
            })],
        )
        .await
        .unwrap();

        // Duplicate view name.
        db.try_mutate(
            version(&db).await,
            vec![Mutation::CreateView(CreateView {
                schema: "luigi".to_string(),
                name: "peach".to_string(),
                sql: "select 2".to_string(),
            })],
        )
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn duplicate_names_no_persist_failures() {
        // <https://github.com/GlareDB/glaredb/issues/577>

        let db = new_catalog().await;
        let initial = version(&db).await;

        // Note that this test uses the resulting state of the mutation for
        // providing the version for followup mutations. This mimics the
        // client-server interaction.
        //
        // Also note that we're using the 'public' schema. This ensures that
        // we're properly handling objects dependent on a builtin.

        // Add connection.
        let state = db
            .try_mutate(
                initial,
                vec![Mutation::CreateConnection(CreateConnection {
                    schema: "public".to_string(),
                    name: "bowser".to_string(),
                    options: ConnectionOptions::Debug(ConnectionOptionsDebug {}),
                })],
            )
            .await
            .unwrap();

        // Duplicate connection, expect failure.
        let _ = db
            .try_mutate(
                state.version,
                vec![Mutation::CreateConnection(CreateConnection {
                    schema: "public".to_string(),
                    name: "bowser".to_string(),
                    options: ConnectionOptions::Debug(ConnectionOptionsDebug {}),
                })],
            )
            .await
            .unwrap_err();

        // Should also fail.
        let _ = db
            .try_mutate(
                state.version,
                vec![Mutation::CreateConnection(CreateConnection {
                    schema: "public".to_string(),
                    name: "bowser".to_string(),
                    options: ConnectionOptions::Debug(ConnectionOptionsDebug {}),
                })],
            )
            .await
            .unwrap_err();

        // Check that the duplicate connection we tried to create is not in the
        // state.
        let state = db.get_state().await.unwrap();
        let ents: Vec<_> = state
            .entries
            .iter()
            .filter(|(_, ent)| ent.get_meta().name == "bowser")
            .collect();
        assert!(
            ents.len() == 1,
            "found more than one 'bowser' entry (found {}): {:?}",
            ents.len(),
            ents
        );
    }
}
