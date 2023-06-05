//! Module for handling the catalog for a single database.
use crate::builtins::{
    BuiltinDatabase, BuiltinSchema, BuiltinTable, BuiltinView, DATABASE_DEFAULT, DEFAULT_SCHEMA,
    FIRST_NON_SCHEMA_ID,
};
use crate::errors::{MetastoreError, Result};
use crate::storage::persist::Storage;
use crate::validation::{
    validate_database_tunnel_support, validate_object_name, validate_table_tunnel_support,
};
use metastoreproto::types::catalog::{
    CatalogEntry, CatalogState, DatabaseEntry, EntryMeta, EntryType, FunctionEntry, FunctionType,
    SchemaEntry, TableEntry, TunnelEntry, ViewEntry,
};
use metastoreproto::types::options::{
    DatabaseOptions, DatabaseOptionsInternal, TableOptions, TunnelOptions,
};
use metastoreproto::types::service::Mutation;
use metastoreproto::types::storage::{ExtraState, PersistedCatalog};
use once_cell::sync::Lazy;
use pgrepr::oid::FIRST_AVAILABLE_ID;
use sqlbuiltins::functions::BUILTIN_TABLE_FUNCS;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};
use tracing::debug;
use uuid::Uuid;

/// Special id indicating that databases have no parents.
const DATABASE_PARENT_ID: u32 = 0;

/// Absolute max number of database objects that can exist in the catalog. This
/// is not configurable when creating a session.
const MAX_DATABASE_OBJECTS: usize = 2048;

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
            entries: guard.entries.as_ref().clone(),
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

/// A thin wrapper around a hashmap for database entries.
///
/// Mutating methods on this type prevent mutating default (builtin) objects.
#[derive(Debug, Clone)]
struct DatabaseEntries(HashMap<u32, CatalogEntry>);

impl DatabaseEntries {
    /// Get an immutable reference to the catalog entry if it exists. Errors if
    /// the entry is builtin.
    fn get(&self, oid: &u32) -> Result<Option<&CatalogEntry>> {
        match self.0.get(oid) {
            Some(ent) if ent.get_meta().builtin => {
                Err(MetastoreError::CannotModifyBuiltin(ent.clone()))
            }
            Some(ent) => Ok(Some(ent)),
            None => Ok(None),
        }
    }

    /// Get a mutable reference to the catalog entry if it exists. Errors if the
    /// entry is builtin.
    fn get_mut(&mut self, oid: &u32) -> Result<Option<&mut CatalogEntry>> {
        match self.0.get_mut(oid) {
            Some(ent) if ent.get_meta().builtin => {
                Err(MetastoreError::CannotModifyBuiltin(ent.clone()))
            }
            Some(ent) => Ok(Some(ent)),
            None => Ok(None),
        }
    }

    /// Remove an entry. Errors if the entry is builtin.
    fn remove(&mut self, oid: &u32) -> Result<Option<CatalogEntry>> {
        if let Some(ent) = self.0.get(oid) {
            if ent.get_meta().builtin {
                return Err(MetastoreError::CannotModifyBuiltin(ent.clone()));
            }
        }

        Ok(self.0.remove(oid))
    }

    /// Insert an entry.
    fn insert(&mut self, oid: u32, ent: CatalogEntry) -> Result<Option<CatalogEntry>> {
        if self.0.len() > MAX_DATABASE_OBJECTS {
            return Err(MetastoreError::MaxNumberOfObjects {
                max: MAX_DATABASE_OBJECTS,
            });
        }

        if let Some(ent) = self.0.get(&oid) {
            if ent.get_meta().builtin {
                return Err(MetastoreError::CannotModifyBuiltin(ent.clone()));
            }
        }

        Ok(self.0.insert(oid, ent))
    }
}

impl From<HashMap<u32, CatalogEntry>> for DatabaseEntries {
    fn from(value: HashMap<u32, CatalogEntry>) -> Self {
        DatabaseEntries(value)
    }
}

impl AsRef<HashMap<u32, CatalogEntry>> for DatabaseEntries {
    fn as_ref(&self) -> &HashMap<u32, CatalogEntry> {
        &self.0
    }
}

/// Determine behavior of creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreatePolicy {
    /// Corresponds to 'IF NOT EXISTS' clauses.
    ///
    /// Suppresses error if there already exists an object with the same name.
    CreateIfNotExists,

    /// Corresponds to 'OR REPLACE' clauses.
    ///
    /// Replaces an existing object with the same name.
    CreateOrReplace,

    /// A plain 'CREATE'.
    ///
    /// Errors if there exists an object with the same name.
    Create,
}

/// Inner state of the catalog.
#[derive(Debug)]
struct State {
    /// Version incremented on every update.
    version: u64,
    /// Next OID to use.
    oid_counter: u32,
    /// All entries in the catalog.
    entries: DatabaseEntries,
    /// Map database names to their ids.
    database_names: HashMap<String, u32>,
    /// Map tunnel names to their ids.
    tunnel_names: HashMap<String, u32>,
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
        let mut state = persisted.state;

        let mut database_names = HashMap::new();
        let mut tunnel_names = HashMap::new();
        let mut schema_names = HashMap::new();
        let mut schema_objects = HashMap::new();

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
        database_names.extend(builtin.database_names);
        schema_names.extend(builtin.schema_names);
        schema_objects.extend(builtin.schema_objects);

        // Rebuild name maps for user objects.
        //
        // All non-database objects are checked to ensure they have non-zero
        // parent ids.
        for (oid, entry) in state
            .entries
            .iter()
            .filter(|(_, ent)| !ent.get_meta().builtin)
        {
            match entry {
                CatalogEntry::Database(database) => {
                    if database.meta.parent != DATABASE_PARENT_ID {
                        return Err(MetastoreError::ObjectHasNonZeroParent {
                            object: *oid,
                            parent: database.meta.parent,
                            object_type: "database",
                        });
                    }

                    database_names.insert(database.meta.name.clone(), *oid);
                }
                CatalogEntry::Tunnel(tunnel) => {
                    if tunnel.meta.parent != DATABASE_PARENT_ID {
                        return Err(MetastoreError::ObjectHasNonZeroParent {
                            object: *oid,
                            parent: tunnel.meta.parent,
                            object_type: "tunnel",
                        });
                    }

                    tunnel_names.insert(tunnel.meta.name.clone(), *oid);
                }
                CatalogEntry::Schema(schema) => {
                    if schema.meta.parent == DATABASE_PARENT_ID {
                        return Err(MetastoreError::ObjectHasInvalidParentId {
                            object: *oid,
                            parent: schema.meta.parent,
                            object_type: "schema",
                        });
                    }

                    schema_names.insert(schema.meta.name.clone(), *oid);
                }
                entry @ CatalogEntry::View(_) | entry @ CatalogEntry::Table(_) => {
                    if entry.get_meta().parent == DATABASE_PARENT_ID {
                        return Err(MetastoreError::ObjectHasInvalidParentId {
                            object: *oid,
                            parent: entry.get_meta().parent,
                            object_type: entry.get_meta().entry_type.as_str(),
                        });
                    }

                    let schema_id = entry.get_meta().parent;

                    let objects = schema_objects.entry(schema_id).or_default();
                    let existing = objects.tables.insert(entry.get_meta().name.clone(), *oid);
                    if let Some(existing) = existing {
                        return Err(MetastoreError::DuplicateNameFoundDuringLoad {
                            name: entry.get_meta().name.clone(),
                            schema: schema_id,
                            first: *oid,
                            second: existing,
                            object_namespace: "table",
                        });
                    }
                }
                CatalogEntry::Function(func) => {
                    if func.meta.parent == DATABASE_PARENT_ID {
                        return Err(MetastoreError::ObjectHasInvalidParentId {
                            object: *oid,
                            parent: func.meta.parent,
                            object_type: func.meta.entry_type.as_str(),
                        });
                    }

                    let objects = schema_objects.entry(func.meta.parent).or_default();
                    let existing = objects.functions.insert(func.meta.name.clone(), *oid);
                    if let Some(existing) = existing {
                        return Err(MetastoreError::DuplicateNameFoundDuringLoad {
                            name: func.meta.name.clone(),
                            schema: func.meta.parent,
                            first: *oid,
                            second: existing,
                            object_namespace: "function",
                        });
                    }
                }
            }
        }

        let internal_state = State {
            version: state.version,
            oid_counter: persisted.extra.oid_counter,
            entries: state.entries.into(),
            database_names,
            tunnel_names,
            schema_names,
            schema_objects,
        };

        Ok(internal_state)
    }

    /// Create a persisted catalog containing only user objects.
    ///
    /// Builtins are added to the catalog when converting from a persisted
    /// catalog.
    fn to_persisted(&self) -> PersistedCatalog {
        PersistedCatalog {
            state: CatalogState {
                version: self.version,
                entries: self
                    .entries
                    .as_ref()
                    .clone()
                    .into_iter()
                    .filter(|(_, ent)| !ent.get_meta().builtin)
                    .collect(),
            },
            extra: ExtraState {
                oid_counter: self.oid_counter,
            },
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
                Mutation::DropDatabase(drop_database) => {
                    // TODO: Dependency checking (for child objects like views, etc.)
                    let if_exists = drop_database.if_exists;
                    let database_id = match self.database_names.remove(&drop_database.name) {
                        None if if_exists => return Ok(()),
                        None => return Err(MetastoreError::MissingDatabase(drop_database.name)),
                        Some(id) => id,
                    };

                    self.entries.remove(&database_id)?.unwrap();
                }
                Mutation::DropTunnel(drop_tunnel) => {
                    let if_exists = drop_tunnel.if_exists;
                    let tunnel_id = match self.tunnel_names.remove(&drop_tunnel.name) {
                        None if if_exists => return Ok(()),
                        None => return Err(MetastoreError::MissingTunnel(drop_tunnel.name)),
                        Some(id) => id,
                    };

                    self.entries.remove(&tunnel_id)?.unwrap();
                }
                Mutation::DropSchema(drop_schema) => {
                    let if_exists = drop_schema.if_exists;
                    let schema_id = match self.schema_names.remove(&drop_schema.name) {
                        None if if_exists => return Ok(()),
                        None => return Err(MetastoreError::MissingNamedSchema(drop_schema.name)),
                        Some(id) => id,
                    };

                    // Check if any child objects exist for this schema
                    match self.schema_objects.get(&schema_id) {
                        Some(so) if so.is_empty() => {
                            self.schema_objects.remove(&schema_id);
                        }
                        Some(_) if drop_schema.cascade => {
                            // Remove all child objects.
                            let objs = self.schema_objects.remove(&schema_id).unwrap(); // Checked above.
                            for child_oid in objs.iter_oids() {
                                // TODO: Dependency checking.
                                self.entries.remove(child_oid)?.unwrap(); // Bug if it doesn't exist.
                            }
                        }
                        None => (), // Empty schema that never had any child objects
                        Some(so) => {
                            return Err(MetastoreError::SchemaHasChildren {
                                schema: schema_id,
                                num_objects: so.num_objects(),
                            });
                        }
                    }

                    self.entries.remove(&schema_id)?.unwrap(); // Bug if doesn't exist.
                }
                // Can drop db objects like tables and views
                Mutation::DropObject(drop_object) => {
                    // TODO: Dependency checking (for child objects like tables, views, etc.)
                    let if_exists = drop_object.if_exists;

                    let schema_id = match self.schema_names.get(&drop_object.schema) {
                        None if if_exists => return Ok(()),
                        None => return Err(MetastoreError::MissingNamedSchema(drop_object.schema)),
                        Some(id) => *id,
                    };

                    let objs = match self.schema_objects.get_mut(&schema_id) {
                        None if if_exists => return Ok(()),
                        None => {
                            return Err(MetastoreError::MissingNamedObject {
                                schema: drop_object.schema,
                                name: drop_object.name,
                            })
                        }
                        Some(objs) => objs,
                    };

                    // TODO: This will need to be tweaked if/when we support
                    // dropping functions.
                    let ent_id = match objs.tables.remove(&drop_object.name) {
                        None if if_exists => return Ok(()),
                        None => {
                            return Err(MetastoreError::MissingNamedObject {
                                schema: drop_object.schema,
                                name: drop_object.name,
                            })
                        }
                        Some(id) => id,
                    };

                    self.entries.remove(&ent_id)?.unwrap(); // Bug if doesn't exist.
                }
                Mutation::CreateExternalDatabase(create_database) => {
                    validate_object_name(&create_database.name)?;
                    match self.database_names.get(&create_database.name) {
                        Some(_) if create_database.if_not_exists => return Ok(()), // Already exists, nothing to do.
                        Some(_) => return Err(MetastoreError::DuplicateName(create_database.name)),
                        None => (),
                    }

                    // Check if the tunnel exists and validate.
                    let tunnel_id = if let Some(tunnel_entry) =
                        self.get_tunnel_entry(create_database.tunnel.as_ref())?
                    {
                        validate_database_tunnel_support(
                            create_database.options.as_str(),
                            tunnel_entry.options.as_str(),
                        )?;
                        Some(tunnel_entry.meta.id)
                    } else {
                        None
                    };

                    // Create new entry
                    let oid = self.next_oid();
                    let ent = DatabaseEntry {
                        meta: EntryMeta {
                            entry_type: EntryType::Database,
                            id: oid,
                            parent: DATABASE_PARENT_ID,
                            name: create_database.name.clone(),
                            builtin: false,
                            external: true,
                        },
                        options: create_database.options,
                        tunnel_id,
                    };
                    self.entries.insert(oid, CatalogEntry::Database(ent))?;

                    // Add to database map
                    self.database_names.insert(create_database.name, oid);
                }
                Mutation::CreateTunnel(create_tunnel) => {
                    validate_object_name(&create_tunnel.name)?;
                    match self.tunnel_names.get(&create_tunnel.name) {
                        Some(_) if create_tunnel.if_not_exists => return Ok(()), // Already exists, nothing to do.
                        Some(_) => return Err(MetastoreError::DuplicateName(create_tunnel.name)),
                        None => (),
                    }

                    // Create new entry
                    let oid = self.next_oid();
                    let ent = TunnelEntry {
                        meta: EntryMeta {
                            entry_type: EntryType::Tunnel,
                            id: oid,
                            // The tunnel, just like databases doesn't have any parent.
                            parent: DATABASE_PARENT_ID,
                            name: create_tunnel.name.clone(),
                            builtin: false,
                            external: true,
                        },
                        options: create_tunnel.options,
                    };
                    self.entries.insert(oid, CatalogEntry::Tunnel(ent))?;

                    // Add to tunnel map
                    self.tunnel_names.insert(create_tunnel.name, oid);
                }
                Mutation::CreateSchema(create_schema) => {
                    validate_object_name(&create_schema.name)?;
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
                            // Schemas can only be created in the default builtin database for now.
                            parent: DATABASE_DEFAULT.oid,
                            name: create_schema.name.clone(),
                            builtin: false,
                            external: false,
                        },
                    };
                    self.entries.insert(oid, CatalogEntry::Schema(ent))?;

                    // Add to name map
                    self.schema_names.insert(create_schema.name, oid);
                }
                Mutation::CreateView(create_view) => {
                    validate_object_name(&create_view.name)?;

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
                            external: false,
                        },
                        sql: create_view.sql,
                        columns: create_view.columns,
                    };

                    let policy = if create_view.or_replace {
                        CreatePolicy::CreateOrReplace
                    } else {
                        CreatePolicy::Create
                    };

                    self.try_insert_table_namespace(
                        CatalogEntry::View(ent),
                        schema_id,
                        oid,
                        policy,
                    )?;
                }
                Mutation::CreateExternalTable(create_ext) => {
                    validate_object_name(&create_ext.name)?;
                    let schema_id = self.get_schema_id(&create_ext.schema)?;

                    // Check if the tunnel exists and validate.
                    let tunnel_id = if let Some(tunnel_entry) =
                        self.get_tunnel_entry(create_ext.tunnel.as_ref())?
                    {
                        validate_table_tunnel_support(
                            create_ext.options.as_str(),
                            tunnel_entry.options.as_str(),
                        )?;
                        Some(tunnel_entry.meta.id)
                    } else {
                        None
                    };

                    // Create new entry.
                    let oid = self.next_oid();
                    let ent = TableEntry {
                        meta: EntryMeta {
                            entry_type: EntryType::Table,
                            id: oid,
                            parent: schema_id,
                            name: create_ext.name.clone(),
                            builtin: false,
                            external: true,
                        },
                        options: create_ext.options,
                        tunnel_id,
                    };

                    let policy = if create_ext.if_not_exists {
                        CreatePolicy::CreateIfNotExists
                    } else {
                        CreatePolicy::Create
                    };

                    self.try_insert_table_namespace(
                        CatalogEntry::Table(ent),
                        schema_id,
                        oid,
                        policy,
                    )?;
                }
                Mutation::AlterTableRename(alter_table_rename) => {
                    validate_object_name(&alter_table_rename.new_name)?;
                    if self.schema_names.contains_key(&alter_table_rename.new_name) {
                        return Err(MetastoreError::DuplicateName(alter_table_rename.new_name));
                    }

                    let schema_id = match self.schema_names.get(&alter_table_rename.schema) {
                        None => {
                            return Err(MetastoreError::MissingNamedSchema(
                                alter_table_rename.schema,
                            ))
                        }
                        Some(id) => *id,
                    };

                    let objs = match self.schema_objects.get_mut(&schema_id) {
                        None => {
                            return Err(MetastoreError::MissingNamedObject {
                                schema: alter_table_rename.schema,
                                name: alter_table_rename.name,
                            })
                        }
                        Some(objs) => objs,
                    };

                    let oid = match objs.tables.remove(&alter_table_rename.name) {
                        None => {
                            return Err(MetastoreError::MissingNamedObject {
                                schema: alter_table_rename.schema,
                                name: alter_table_rename.name,
                            })
                        }
                        Some(id) => id,
                    };

                    let mut table = match self.entries.remove(&oid)?.unwrap() {
                        CatalogEntry::Table(ent) => ent,
                        other => unreachable!("unexpected entry type: {:?}", other),
                    };

                    table.meta.name = alter_table_rename.new_name;

                    self.try_insert_table_namespace(
                        CatalogEntry::Table(table.clone()),
                        schema_id,
                        table.meta.id,
                        CreatePolicy::Create,
                    )?;
                }
                Mutation::AlterDatabaseRename(alter_database_rename) => {
                    validate_object_name(&alter_database_rename.new_name)?;
                    if self
                        .database_names
                        .contains_key(&alter_database_rename.new_name)
                    {
                        return Err(MetastoreError::DuplicateName(
                            alter_database_rename.new_name,
                        ));
                    }

                    let oid = match self.database_names.remove(&alter_database_rename.name) {
                        None => {
                            return Err(MetastoreError::MissingDatabase(
                                alter_database_rename.name,
                            ));
                        }
                        Some(objs) => objs,
                    };

                    let ent = self.entries.get_mut(&oid)?.unwrap();
                    ent.get_meta_mut().name = alter_database_rename.new_name.clone();

                    // Add to database map
                    self.database_names
                        .insert(alter_database_rename.new_name, oid);
                }
                Mutation::AlterTunnelRotateKeys(alter_tunnel_rotate_keys) => {
                    let oid = match self.tunnel_names.get(&alter_tunnel_rotate_keys.name) {
                        None if alter_tunnel_rotate_keys.if_exists => return Ok(()),
                        None => {
                            return Err(MetastoreError::MissingTunnel(
                                alter_tunnel_rotate_keys.name,
                            ))
                        }
                        Some(oid) => oid,
                    };

                    match self.entries.get_mut(oid)?.unwrap() {
                        CatalogEntry::Tunnel(tunnel_entry) => match &mut tunnel_entry.options {
                            TunnelOptions::Ssh(tunnel_options_ssh) => {
                                tunnel_options_ssh.ssh_key = alter_tunnel_rotate_keys.new_ssh_key;
                            }
                            opt => {
                                return Err(MetastoreError::TunnelNotSupportedForAction {
                                    tunnel: opt.to_string(),
                                    action: "rotating keys",
                                })
                            }
                        },
                        _ => unreachable!("entry should be a tunnel"),
                    };
                }
            }
        }

        Ok(())
    }

    /// Try to insert an entry for a schema within the "table" namespace.
    ///
    /// Errors depending on the create policy.
    fn try_insert_table_namespace(
        &mut self,
        ent: CatalogEntry,
        schema_id: u32,
        oid: u32,
        create_policy: CreatePolicy,
    ) -> Result<()> {
        // Insert new entry for schema. Checks if there exists an
        // object with the same name.
        let objs = self.schema_objects.entry(schema_id).or_default();

        match create_policy {
            CreatePolicy::CreateIfNotExists => {
                if objs.tables.contains_key(&ent.get_meta().name) {
                    return Ok(());
                }
                objs.tables.insert(ent.get_meta().name.clone(), oid);
                self.entries.insert(oid, ent)?;
            }
            CreatePolicy::CreateOrReplace => {
                if let Some(existing_oid) = objs.tables.insert(ent.get_meta().name.clone(), oid) {
                    self.entries.remove(&existing_oid)?;
                }
                self.entries.insert(oid, ent)?;
            }
            CreatePolicy::Create => {
                if objs.tables.contains_key(&ent.get_meta().name) {
                    return Err(MetastoreError::DuplicateName(ent.get_meta().name.clone()));
                }
                objs.tables.insert(ent.get_meta().name.clone(), oid);
                self.entries.insert(oid, ent)?;
            }
        }

        Ok(())
    }

    fn get_schema_id(&self, name: &str) -> Result<u32> {
        self.schema_names
            .get(name)
            .cloned()
            .ok_or_else(|| MetastoreError::MissingNamedSchema(name.to_string()))
    }

    fn get_tunnel_entry(&self, tunnel_name: Option<&String>) -> Result<Option<&TunnelEntry>> {
        let tunnel_entry = if let Some(tunnel) = tunnel_name {
            let tunnel_id = *self
                .tunnel_names
                .get(tunnel)
                .ok_or(MetastoreError::MissingTunnel(tunnel.clone()))?;
            let tunnel_entry = match self.entries.get(&tunnel_id)?.expect("entry should exist") {
                CatalogEntry::Tunnel(tunnel_entry) => tunnel_entry,
                ent => unreachable!("entry should be a tunnel entry but found: {ent:?}"),
            };
            Some(tunnel_entry)
        } else {
            None
        };
        Ok(tunnel_entry)
    }
}

/// Holds names to object ids for a single schema.
#[derive(Debug, Default, Clone)]
struct SchemaObjects {
    /// The "table" namespace in this schema. Views and external tables should
    /// be included in this namespace.
    ///
    /// Maps names to object ids.
    tables: HashMap<String, u32>,

    /// The "function" namespace.
    functions: HashMap<String, u32>,
}

impl SchemaObjects {
    fn is_empty(&self) -> bool {
        self.tables.is_empty() && self.functions.is_empty()
    }

    fn iter_oids(&self) -> impl Iterator<Item = &u32> {
        self.tables.values().chain(self.functions.values())
    }

    fn num_objects(&self) -> usize {
        self.tables.len() + self.functions.len()
    }
}

/// Catalog with builtin objects. Used during database catalog initialization.
#[derive(Clone)]
struct BuiltinCatalog {
    /// All entries in the catalog.
    entries: HashMap<u32, CatalogEntry>,
    /// Map database names to their ids.
    database_names: HashMap<String, u32>,
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
        let mut database_names = HashMap::new();
        let mut schema_names = HashMap::new();
        let mut schema_objects = HashMap::new();

        for database in BuiltinDatabase::builtins() {
            database_names.insert(database.name.to_string(), database.oid);
            entries.insert(
                database.oid,
                CatalogEntry::Database(DatabaseEntry {
                    meta: EntryMeta {
                        entry_type: EntryType::Database,
                        id: database.oid,
                        parent: DATABASE_PARENT_ID,
                        name: database.name.to_string(),
                        builtin: true,
                        external: false,
                    },
                    options: DatabaseOptions::Internal(DatabaseOptionsInternal {}),
                    tunnel_id: None,
                }),
            );
        }

        for schema in BuiltinSchema::builtins() {
            schema_names.insert(schema.name.to_string(), schema.oid);
            schema_objects.insert(schema.oid, SchemaObjects::default());
            entries.insert(
                schema.oid,
                CatalogEntry::Schema(SchemaEntry {
                    meta: EntryMeta {
                        entry_type: EntryType::Schema,
                        id: schema.oid,
                        parent: DATABASE_DEFAULT.oid,
                        name: schema.name.to_string(),
                        builtin: true,
                        external: false,
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
                        external: false,
                    },
                    options: TableOptions::new_internal(table.columns.clone()),
                    tunnel_id: None,
                }),
            );
            schema_objects
                .get_mut(schema_id)
                .unwrap()
                .tables
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
                        external: false,
                    },
                    sql: view.sql.to_string(),
                    columns: Vec::new(),
                }),
            );
            schema_objects
                .get_mut(schema_id)
                .unwrap()
                .tables
                .insert(view.name.to_string(), oid);

            oid += 1;
        }

        for func in BUILTIN_TABLE_FUNCS.iter_funcs() {
            // Put them all in the default schema.
            let schema_id = schema_names
                .get(DEFAULT_SCHEMA)
                .ok_or_else(|| MetastoreError::MissingNamedSchema(DEFAULT_SCHEMA.to_string()))?;
            entries.insert(
                oid,
                CatalogEntry::Function(FunctionEntry {
                    meta: EntryMeta {
                        entry_type: EntryType::Function,
                        id: oid,
                        parent: *schema_id,
                        name: func.name().to_string(),
                        builtin: true,
                        external: false,
                    },
                    func_type: FunctionType::TableReturning,
                }),
            );
            schema_objects
                .get_mut(schema_id)
                .unwrap()
                .functions
                .insert(func.name().to_string(), oid);

            oid += 1;
        }

        Ok(BuiltinCatalog {
            entries,
            database_names,
            schema_names,
            schema_objects,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtins::DEFAULT_CATALOG;
    use crate::storage::persist::Storage;
    use metastoreproto::types::options::DatabaseOptionsDebug;
    use metastoreproto::types::options::TableOptionsDebug;
    use metastoreproto::types::service::AlterDatabaseRename;
    use metastoreproto::types::service::DropDatabase;
    use metastoreproto::types::service::{
        CreateExternalDatabase, CreateExternalTable, CreateSchema, CreateView, DropSchema,
    };
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
                if_exists: false,
                cascade: false,
            })],
        )
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn drop_missing_schema_if_exists() {
        let db = new_catalog().await;

        db.try_mutate(
            version(&db).await,
            vec![Mutation::DropSchema(DropSchema {
                name: "yoshi".to_string(),
                if_exists: true,
                cascade: false,
            })],
        )
        .await
        .unwrap();
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
                    or_replace: false,
                    columns: Vec::new(),
                })
            })
            .collect();

        db.try_mutate(version(&db).await, mutations).await.unwrap();

        let state = db.get_state().await.unwrap();

        let mut found = HashSet::new();
        for (_, ent) in state.entries {
            if !matches!(ent, CatalogEntry::Schema(_)) {
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
                if_exists: false,
                cascade: false,
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
    async fn drop_schema_cascade() {
        let db = new_catalog().await;

        let state = db
            .try_mutate(
                version(&db).await,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "mushroom".to_string(),
                })],
            )
            .await
            .unwrap();

        let state = db
            .try_mutate(
                state.version,
                vec![Mutation::CreateView(CreateView {
                    schema: "mushroom".to_string(),
                    name: "bowser".to_string(),
                    sql: "select 1".to_string(),
                    or_replace: false,
                    columns: Vec::new(),
                })],
            )
            .await
            .unwrap();

        // Drop schema cascade. containing a view.
        db.try_mutate(
            state.version,
            vec![Mutation::DropSchema(DropSchema {
                name: "mushroom".to_string(),
                if_exists: false,
                cascade: true,
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
                or_replace: false,
                columns: Vec::new(),
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
                or_replace: false,
                columns: Vec::new(),
            })],
        )
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn replace_view() {
        let db = new_catalog().await;

        // Add view.
        db.try_mutate(
            version(&db).await,
            vec![Mutation::CreateView(CreateView {
                schema: "public".to_string(),
                name: "wario".to_string(),
                sql: "select 1".to_string(),
                or_replace: false,
                columns: Vec::new(),
            })],
        )
        .await
        .unwrap();

        // Try to replace with specifying 'or replace'.
        db.try_mutate(
            version(&db).await,
            vec![Mutation::CreateView(CreateView {
                schema: "public".to_string(),
                name: "wario".to_string(),
                sql: "select 2".to_string(),
                or_replace: false,
                columns: Vec::new(),
            })],
        )
        .await
        .unwrap_err();

        // Replace view.
        db.try_mutate(
            version(&db).await,
            vec![Mutation::CreateView(CreateView {
                schema: "public".to_string(),
                name: "wario".to_string(),
                sql: "select 3".to_string(),
                or_replace: true,
                columns: Vec::new(),
            })],
        )
        .await
        .unwrap();
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

        // Add schema.
        let state = db
            .try_mutate(
                initial,
                vec![Mutation::CreateView(CreateView {
                    schema: "public".to_string(),
                    name: "bowser".to_string(),
                    sql: "select 1".to_string(),
                    or_replace: false,
                    columns: Vec::new(),
                })],
            )
            .await
            .unwrap();

        // Duplicate connection, expect failure.
        let _ = db
            .try_mutate(
                state.version,
                vec![Mutation::CreateView(CreateView {
                    schema: "public".to_string(),
                    name: "bowser".to_string(),
                    sql: "select 1".to_string(),
                    or_replace: false,
                    columns: Vec::new(),
                })],
            )
            .await
            .unwrap_err();

        // Should also fail.
        let _ = db
            .try_mutate(
                state.version,
                vec![Mutation::CreateView(CreateView {
                    schema: "public".to_string(),
                    name: "bowser".to_string(),
                    sql: "select 1".to_string(),
                    or_replace: false,
                    columns: Vec::new(),
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

    #[tokio::test]
    async fn duplicate_table_names_if_not_exists() {
        let db = new_catalog().await;
        let initial = version(&db).await;

        // Add schema.
        let state = db
            .try_mutate(
                initial,
                vec![Mutation::CreateSchema(CreateSchema {
                    name: "mushroom".to_string(),
                })],
            )
            .await
            .unwrap();

        // Try to add two tables with the same name, each with "if not exists"
        // set to true.
        let mutation = Mutation::CreateExternalTable(CreateExternalTable {
            schema: "mushroom".to_string(),
            name: "bowser".to_string(),
            options: TableOptions::Debug(TableOptionsDebug {
                table_type: String::new(),
            }),
            if_not_exists: true,
            tunnel: None,
        });
        let _ = db
            .try_mutate(state.version, vec![mutation.clone(), mutation])
            .await
            .unwrap();

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

    #[tokio::test]
    async fn duplicate_database_names() {
        let db = new_catalog().await;
        let initial = version(&db).await;

        let state = db
            .try_mutate(
                initial,
                vec![Mutation::CreateExternalDatabase(CreateExternalDatabase {
                    name: "bq".to_string(),
                    options: DatabaseOptions::Debug(DatabaseOptionsDebug {}),
                    if_not_exists: false,
                    tunnel: None,
                })],
            )
            .await
            .unwrap();

        // Should fail if "if not exists" set to false.
        let _ = db
            .try_mutate(
                state.version,
                vec![Mutation::CreateExternalDatabase(CreateExternalDatabase {
                    name: "bq".to_string(),
                    options: DatabaseOptions::Debug(DatabaseOptionsDebug {}),
                    if_not_exists: false,
                    tunnel: None,
                })],
            )
            .await
            .unwrap_err();

        // Should pass if "if not exists" set to true.
        let state = db
            .try_mutate(
                state.version,
                vec![Mutation::CreateExternalDatabase(CreateExternalDatabase {
                    name: "bq".to_string(),
                    options: DatabaseOptions::Debug(DatabaseOptionsDebug {}),
                    if_not_exists: true,
                    tunnel: None,
                })],
            )
            .await
            .unwrap();

        // Drop database.
        let state = db
            .try_mutate(
                state.version,
                vec![Mutation::DropDatabase(DropDatabase {
                    name: "bq".to_string(),
                    if_exists: false,
                })],
            )
            .await
            .unwrap();

        // Now should be able to create new database with name.
        let _state = db
            .try_mutate(
                state.version,
                vec![Mutation::CreateExternalDatabase(CreateExternalDatabase {
                    name: "bq".to_string(),
                    options: DatabaseOptions::Debug(DatabaseOptionsDebug {}),
                    if_not_exists: false,
                    tunnel: None,
                })],
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn drop_database_if_exists() {
        let db = new_catalog().await;
        let initial = version(&db).await;

        let _state = db
            .try_mutate(
                initial,
                vec![Mutation::DropDatabase(DropDatabase {
                    name: "doesntexist".to_string(),
                    if_exists: true,
                })],
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn try_modify_default_db() {
        let db = new_catalog().await;
        let initial = version(&db).await;

        let e = db
            .try_mutate(
                initial,
                vec![Mutation::AlterDatabaseRename(AlterDatabaseRename {
                    name: DEFAULT_CATALOG.to_string(),
                    new_name: "hello".to_string(),
                })],
            )
            .await
            .unwrap_err();

        match e {
            MetastoreError::CannotModifyBuiltin(ent) => {
                assert_eq!(ent.get_meta().name, DEFAULT_CATALOG);
            }
            e => panic!("unexpected error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn separate_function_namespace() {
        let db = new_catalog().await;
        let initial = version(&db).await;

        // Try to create a table with the same name as an existing builtin
        // function (read_postgres).
        let _state = db
            .try_mutate(
                initial,
                vec![Mutation::CreateExternalTable(CreateExternalTable {
                    schema: "public".to_string(),
                    name: "read_postgres".to_string(),
                    options: TableOptions::Debug(TableOptionsDebug {
                        table_type: String::new(),
                    }),
                    if_not_exists: true,
                    tunnel: None,
                })],
            )
            .await
            .unwrap();
    }
}
