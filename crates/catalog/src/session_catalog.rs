use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::{MemTable, TableProvider};
use parking_lot::Mutex;
use protogen::metastore::types::catalog::{
    CatalogEntry,
    CatalogState,
    CredentialsEntry,
    DatabaseEntry,
    DeploymentMetadata,
    EntryMeta,
    EntryType,
    FunctionEntry,
    FunctionType,
    SchemaEntry,
    SourceAccessMode,
    TableEntry,
    TunnelEntry,
};
use protogen::metastore::types::options::{InternalColumnDefinition, TableOptionsInternal};
use tracing::debug;

use super::client::MetastoreClientHandle;
use crate::errors::Result;

/// Configuration for letting the catalog know how to resolve certain items.
///
/// Note this was created to avoid needing to import the constants from
/// `sqlbuiltins` since it would create a dependency cycle. This may be removed
/// in the future.
#[derive(Debug, Clone, Copy)]
pub struct ResolveConfig {
    /// Default schema to use when resolving a catalog entry.
    pub default_schema_oid: u32,
    /// Schema to use for the session schema for holding temporary items.
    pub session_schema_oid: u32,
}

/// The session local catalog.
///
/// This catalog should be stored on the "client" side and periodically updated
/// from the remote state provided by metastore.
#[derive(Clone, Debug)]
pub struct SessionCatalog {
    /// Optional alias for referencing objects in this catalog
    alias: Option<String>,
    /// The state retrieved from a remote Metastore.
    state: Arc<CatalogState>,
    /// Map database names to their ids.
    database_names: HashMap<String, u32>,
    /// Map tunnel names to their ids.
    tunnel_names: HashMap<String, u32>,
    /// Map credentials names to their ids.
    credentials_names: HashMap<String, u32>,
    /// Map schema names to their ids.
    schema_names: HashMap<String, u32>,
    /// Map schema IDs to objects in the schema.
    schema_objects: HashMap<u32, SchemaObjects>,
    /// Config for resolving entries.
    resolve_conf: ResolveConfig,
    /// Catalog for holding temporary session objects.
    temp: TempCatalog,
}

impl SessionCatalog {
    /// Create a new session catalog with an initial state.
    pub fn new(state: Arc<CatalogState>, resolve_conf: ResolveConfig) -> SessionCatalog {
        let mut catalog = SessionCatalog {
            alias: None,
            state,
            database_names: HashMap::new(),
            tunnel_names: HashMap::new(),
            credentials_names: HashMap::new(),
            schema_names: HashMap::new(),
            schema_objects: HashMap::new(),
            resolve_conf,
            temp: TempCatalog::new(resolve_conf),
        };
        catalog.rebuild_name_maps();
        catalog
    }

    pub fn new_with_alias(
        state: Arc<CatalogState>,
        resolve_conf: ResolveConfig,
        alias: String,
    ) -> SessionCatalog {
        let catalog = Self::new(state, resolve_conf);
        catalog.with_alias(alias)
    }

    pub fn with_alias(mut self, alias: String) -> SessionCatalog {
        self.alias = Some(alias);
        self
    }

    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    /// Get the version of this catalog state.
    pub fn version(&self) -> u64 {
        self.state.version
    }

    /// Returns the deployment metadata.
    pub fn deployment_metadata(&self) -> DeploymentMetadata {
        self.state.deployment.clone()
    }

    pub fn get_state(&self) -> &Arc<CatalogState> {
        &self.state
    }

    /// Get a reference to the temporary catalog.
    pub fn get_temp_catalog(&self) -> &TempCatalog {
        &self.temp
    }

    pub fn resolve_table(&self, _database: &str, schema: &str, name: &str) -> Option<&TableEntry> {
        let schema_id = self.schema_names.get(schema)?;
        let obj = self.schema_objects.get(schema_id)?;
        let obj_id = obj.objects.get(name)?;

        let ent = self.state.entries.get(obj_id)?;

        match ent {
            CatalogEntry::Table(table) => Some(table),
            _ => None,
        }
    }

    /// Resolve a database by name.
    pub fn resolve_database(&self, name: &str) -> Option<&DatabaseEntry> {
        // This function will panic if certain invariants aren't held:
        //
        // - If the name is found in the name map, then the associated id must
        //   exist in the catalog state.
        // - The catalog entry type that the id points to must be a database.

        let id = self.database_names.get(name)?;
        let ent = self
            .state
            .entries
            .get(id)
            .expect("database name points to invalid id");

        match ent {
            CatalogEntry::Database(ent) => Some(ent),
            _ => panic!(
                "entry type not database; name: {}, id: {}, type: {:?}",
                name,
                id,
                ent.entry_type(),
            ),
        }
    }

    /// Resolve a tunnel by name.
    pub fn resolve_tunnel(&self, name: &str) -> Option<&TunnelEntry> {
        // Similar invariants as `resolve_database`. If we find an entry in the
        // tunnel map, it must exist in the state and must be a tunnel.

        let id = self.tunnel_names.get(name)?;
        let ent = self
            .state
            .entries
            .get(id)
            .expect("tunnel name points to invalid id");

        match ent {
            CatalogEntry::Tunnel(ent) => Some(ent),
            _ => panic!(
                "entry type not tunnel; name: {}, id: {}, type: {:?}",
                name,
                id,
                ent.entry_type(),
            ),
        }
    }

    /// Resolve a credentials by name.
    pub fn resolve_credentials(&self, name: &str) -> Option<&CredentialsEntry> {
        // Similar invariants as `resolve_database`. If we find an entry
        // in the credentials map, it must exist in the state and must be
        // a credentials.

        let id = self.credentials_names.get(name)?;
        let ent = self
            .state
            .entries
            .get(id)
            .expect("credentials name points to invalid id");

        match ent {
            CatalogEntry::Credentials(ent) => Some(ent),
            _ => panic!(
                "entry type not credentials; name: {}, id: {}, type: {:?}",
                name,
                id,
                ent.entry_type(),
            ),
        }
    }

    /// Resolve a schema by name.
    pub fn resolve_schema(&self, name: &str) -> Option<&SchemaEntry> {
        // Similar invariants as `resolve_database`. If we find an entry in the
        // schema map, it must exist in the state and must be a schema.

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

    /// Resolve builtin table functions from 'public' schema
    pub fn resolve_builtin_table_function(&self, name: &str) -> Option<FunctionEntry> {
        let obj = self
            .schema_objects
            .get(&self.resolve_conf.default_schema_oid)?;
        let obj_id = obj.objects.get(name)?;

        let ent = self
            .state
            .entries
            .get(obj_id)
            .expect("object name points to invalid function");

        match ent {
            CatalogEntry::Function(function)
                if function.meta.builtin && function.func_type == FunctionType::TableReturning =>
            {
                Some(function.clone())
            }
            _ => None,
        }
    }

    /// Resolve an entry by schema name and object name.
    ///
    /// Note that this will never return a schema entry.
    pub fn resolve_entry(
        &self,
        _database: &str,
        schema: &str,
        name: &str,
    ) -> Option<&CatalogEntry> {
        let schema_id = self.schema_names.get(schema)?;
        let obj = self.schema_objects.get(schema_id)?;
        let obj_id = obj.objects.get(name)?;

        let ent = self
            .state
            .entries
            .get(obj_id)
            .expect("object name points to invalid id");

        assert!(
            // Should be an object inside a schema.
            !matches!(
                ent,
                CatalogEntry::Database(_) | CatalogEntry::Tunnel(_) | CatalogEntry::Schema(_)
            )
        );

        Some(ent)
    }

    /// Get an entry by its id.
    pub fn get_by_oid(&self, oid: u32) -> Option<&CatalogEntry> {
        self.state.entries.get(&oid)
    }

    /// Get an entry by its id, along with its parent entry.
    pub fn get_namespaced_by_oid(&self, oid: u32) -> Option<NamespacedCatalogEntry<'_>> {
        let ent = self.get_by_oid(oid)?;
        Some(self.as_namespaced_entry(ent))
    }

    /// Iterate over all entries in this catalog.
    ///
    /// All non-database entries will also include an entry pointing to its
    /// parent.
    pub fn iter_entries(&self) -> impl Iterator<Item = NamespacedCatalogEntry> {
        self.state
            .entries
            .values()
            .map(|entry| self.as_namespaced_entry(entry))
    }

    /// Maybe refresh this catalog's state using the metastore client.
    ///
    /// If the client isn't configured, nothing is done.
    pub async fn maybe_refresh_state(
        &mut self,
        client: Option<&MetastoreClientHandle>,
        force_refresh: bool,
    ) -> Result<()> {
        let client = match client {
            Some(client) => client,
            None => return Ok(()),
        };

        if force_refresh {
            debug!("refreshed cached catalog state as per force_catalog_refresh");
            client.refresh_cached_state().await?;
        }

        // Swap out cached catalog if a newer one was fetched.
        //
        // Note that when we have transactions, we should move this to only
        // swapping states between transactions.
        if client.version_hint() != self.version() {
            let new_state = client.get_cached_state().await?;
            debug!(old_version = %self.version(), new_version = %new_state.version, "swapping catalog state for session");
            self.swap_state(new_state);
        }

        Ok(())
    }

    /// Swap the underlying state of the catalog.
    ///
    /// NOTE: This does not check if the new state is actually from a new
    /// catalog.
    pub fn swap_state(&mut self, new_state: Arc<CatalogState>) {
        debug!(old_version = %self.state.version, new_version = %new_state.version, "swapping session catalog");

        self.state = new_state;
        self.rebuild_name_maps();
    }

    fn as_namespaced_entry<'a>(&'a self, ent: &'a CatalogEntry) -> NamespacedCatalogEntry<'a> {
        let parent_entry = match ent {
            // Explicitly mention all the options to accidentally not leave anything here.
            CatalogEntry::Database(_) | CatalogEntry::Tunnel(_) | CatalogEntry::Credentials(_) => {
                None
            }
            CatalogEntry::Schema(_)
            | CatalogEntry::Table(_)
            | CatalogEntry::View(_)
            | CatalogEntry::Function(_) => {
                Some(self.state.entries.get(&ent.get_meta().parent).unwrap()) // Bug if it doesn't exist.
            }
        };
        NamespacedCatalogEntry {
            oid: ent.get_meta().id,
            builtin: ent.get_meta().builtin,
            parent_entry,
            entry: ent,
        }
    }

    fn rebuild_name_maps(&mut self) {
        self.database_names.clear();
        self.tunnel_names.clear();
        self.credentials_names.clear();
        self.schema_names.clear();
        self.schema_objects.clear();

        for (id, ent) in &self.state.entries {
            let name = ent.get_meta().name.clone();

            match ent {
                CatalogEntry::Database(_) => {
                    self.database_names.insert(name, *id);
                }
                CatalogEntry::Tunnel(_) => {
                    self.tunnel_names.insert(name, *id);
                }
                CatalogEntry::Credentials(_) => {
                    self.credentials_names.insert(name, *id);
                }
                CatalogEntry::Schema(_) => {
                    self.schema_names.insert(name, *id);
                }
                CatalogEntry::Table(_) | CatalogEntry::View(_) | CatalogEntry::Function(_) => {
                    let schema_id = ent.get_meta().parent;
                    let ent = self.schema_objects.entry(schema_id).or_default();
                    ent.objects.insert(name, *id);
                }
            }
        }
    }
}

/// Holds names to object ids for a single schema.
#[derive(Clone, Debug, Default)]
struct SchemaObjects {
    /// Maps names to ids in this schema.
    objects: HashMap<String, u32>,
}

/// An entry that's possibly namespaces by a schema.
#[derive(Debug)]
pub struct NamespacedCatalogEntry<'a> {
    /// The OID of the entry.
    pub oid: u32,
    /// Whether or not this entry is builtin.
    pub builtin: bool,
    /// The parent entry for this entry. This will be `None` when the entry is a
    /// root entry like a database or a tunnel.
    pub parent_entry: Option<&'a CatalogEntry>,
    /// The entry.
    pub entry: &'a CatalogEntry,
}

impl NamespacedCatalogEntry<'_> {
    /// Get the entry type for this entry.
    pub fn entry_type(&self) -> EntryType {
        self.entry.get_meta().entry_type
    }
}

/// Temporary objects that are dropped when the session is dropped.
///
/// Cheaply cloneable with interior mutability.
#[derive(Debug, Clone)]
pub struct TempCatalog {
    inner: Arc<Mutex<TempObjectsInner>>,
    temp_schema_oid: u32,
}

impl TempCatalog {
    fn new(resolve_conf: ResolveConfig) -> Self {
        TempCatalog {
            inner: Arc::new(Mutex::new(TempObjectsInner::default())),
            temp_schema_oid: resolve_conf.session_schema_oid,
        }
    }
}

#[derive(Debug, Default)]
struct TempObjectsInner {
    tables: HashMap<String, Arc<MemTable>>,
}

impl TempCatalog {
    pub fn resolve_temp_table(&self, name: &str) -> Option<TableEntry> {
        let inner = self.inner.lock();
        inner.tables.get(name).map(|tbl| {
            let schema = tbl.schema();
            let columns: Vec<_> = schema
                .fields()
                .iter()
                .map(|f| {
                    let name = f.name().to_string();
                    let ty = f.data_type();
                    InternalColumnDefinition {
                        name,
                        nullable: f.is_nullable(),
                        arrow_type: ty.clone(),
                    }
                })
                .collect();

            TableEntry {
                meta: EntryMeta {
                    entry_type: EntryType::Table,
                    id: 0,
                    parent: self.temp_schema_oid,
                    name: name.to_string(),
                    builtin: false,
                    external: false,
                    is_temp: true,
                },
                options: TableOptionsInternal {
                    columns: columns.clone(),
                }
                .into(),
                tunnel_id: None,
                access_mode: SourceAccessMode::ReadWrite,
                columns: Some(columns),
            }
        })
    }

    pub fn put_temp_table(&self, name: String, table: Arc<MemTable>) {
        let mut inner = self.inner.lock();
        inner.tables.insert(name, table);
    }

    pub fn get_temp_table_provider(&self, name: &str) -> Option<Arc<MemTable>> {
        self.inner.lock().tables.get(name).cloned()
    }

    pub fn drop_table(&self, name: &str) {
        let mut inner = self.inner.lock();
        inner.tables.remove(name);
    }

    /// Returns true if the table exists in the temp catalog.
    pub fn contains_table(&self, name: &str) -> bool {
        let inner = self.inner.lock();
        inner.tables.contains_key(name)
    }

    pub fn get_table_entries(&self) -> Vec<TableEntry> {
        let inner = self.inner.lock();
        let mut ents = Vec::with_capacity(inner.tables.len());
        for name in inner.tables.keys() {
            ents.push(TableEntry {
                meta: EntryMeta {
                    entry_type: EntryType::Table,
                    id: 0,
                    parent: self.temp_schema_oid,
                    name: name.to_string(),
                    builtin: false,
                    external: false,
                    is_temp: true,
                },
                options: TableOptionsInternal {
                    columns: Vec::new(),
                }
                .into(),
                tunnel_id: None,
                access_mode: SourceAccessMode::ReadWrite,
                columns: None,
            });
        }

        ents
    }
}
