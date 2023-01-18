//! Module for handling the catalog for a single database.
use crate::errors::{MetastoreError, Result};
use crate::types::catalog::{
    CatalogEntry, CatalogState, ConnectionEntry, EntryMeta, EntryType, ExternalTableEntry,
    SchemaEntry, ViewEntry,
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
#[derive(Debug, Default)]
struct SchemaObjects {
    /// Maps names to ids in this schema.
    objects: HashMap<String, u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::service::{CreateSchema, CreateView, DropSchema};
    use std::collections::HashSet;

    async fn new_catalog() -> DatabaseCatalog {
        DatabaseCatalog::open(Uuid::new_v4()).await.unwrap()
    }

    async fn version(db: &DatabaseCatalog) -> u64 {
        db.get_state().await.unwrap().version
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

        assert_eq!(10, found.len(), "missing entries: found: {:?}", found,)
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
}
