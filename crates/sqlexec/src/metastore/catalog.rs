use protogen::metastore::strategy::ResolveErrorStrategy;
use protogen::metastore::types::catalog::{
    CatalogEntry, CatalogState, CredentialsEntry, DatabaseEntry, DeploymentMetadata, EntryType,
    SchemaEntry, TableEntry, TunnelEntry,
};
use protogen::metastore::types::options::TableOptions;
use protogen::metastore::types::service::Mutation;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use super::client::{MetastoreClientError, MetastoreClientHandle};

#[derive(Debug, thiserror::Error)]
pub enum SessionCatalogError {
    #[error("Metastore client not configured")]
    MetastoreClientNotConfigured,

    #[error(transparent)]
    WorkerClientError(#[from] MetastoreClientError),
}

type Result<T, E = SessionCatalogError> = std::result::Result<T, E>;

/// The session local catalog.
///
/// This catalog should be stored on the "client" side and periodically updated
/// from the remote state provided by metastore.
pub struct SessionCatalog {
    /// The client to the remote metastore.
    ///
    /// If `None`, certain operations cannot be done.
    ///
    /// This will eventually allow other catalog sources too (like rpcsrv).
    metastore_client: Option<MetastoreClientHandle>,

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
}

impl SessionCatalog {
    /// Create a new session catalog with an initial state.
    pub fn new(state: Arc<CatalogState>) -> SessionCatalog {
        let mut catalog = SessionCatalog {
            metastore_client: None,
            state,
            database_names: HashMap::new(),
            tunnel_names: HashMap::new(),
            credentials_names: HashMap::new(),
            schema_names: HashMap::new(),
            schema_objects: HashMap::new(),
        };
        catalog.rebuild_name_maps();
        catalog
    }

    /// Create a session with an initial state along with the client to
    /// metastore.
    pub fn new_with_client(
        state: Arc<CatalogState>,
        client: MetastoreClientHandle,
    ) -> SessionCatalog {
        let mut catalog = Self::new(state);
        catalog.metastore_client = Some(client);
        catalog
    }

    pub fn get_metastore_client(&self) -> Option<&MetastoreClientHandle> {
        self.metastore_client.as_ref()
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

    pub fn resolve_native_table(
        &self,
        _database: &str,
        schema: &str,
        name: &str,
    ) -> Option<&TableEntry> {
        let schema_id = self.schema_names.get(schema)?;
        let obj = self.schema_objects.get(schema_id)?;
        let obj_id = obj.objects.get(name)?;

        let ent = self.state.entries.get(obj_id)?;

        match ent {
            CatalogEntry::Table(table) => match &table.options {
                TableOptions::Internal(_) => Some(table),
                _ => None,
            },
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
    pub async fn maybe_refresh_state(&mut self, force_refresh: bool) -> Result<()> {
        let client = match &self.metastore_client {
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
            debug!(version = %self.version(), "swapping catalog state for session");
            let new_state = client.get_cached_state().await?;
            self.swap_state(new_state);
        }

        Ok(())
    }

    /// Mutate the catalog if possible.
    ///
    /// Errors if the metastore client isn't configured.
    ///
    /// This will retry mutations if we were working with an out of date
    /// catalog.
    pub async fn mutate(&mut self, mutations: impl IntoIterator<Item = Mutation>) -> Result<()> {
        let client = match &self.metastore_client {
            Some(client) => client.clone(), // Cloned to make the below retry stuff easier.
            None => return Err(SessionCatalogError::MetastoreClientNotConfigured),
        };

        // Note that when we have transactions, these shouldn't be sent until
        // commit.
        let mutations: Vec<_> = mutations.into_iter().collect();

        let state = match client.try_mutate(self.version(), mutations.clone()).await {
            Ok(state) => state,
            Err(MetastoreClientError::MetastoreTonic {
                strategy: ResolveErrorStrategy::FetchCatalogAndRetry,
                message,
            }) => {
                // Go ahead and refetch the catalog and retry the mutation.
                //
                // Note that this relies on metastore _always_ being stricter
                // when validating mutations. What this means is that retrying
                // here should be semantically equivalent to manually refreshing
                // the catalog and rerunning and replanning the query.
                debug!(error_message = message, "retrying mutations");

                client.refresh_cached_state().await?;
                let state = client.get_cached_state().await?;
                let version = state.version;
                self.swap_state(state);

                client.try_mutate(version, mutations).await?
            }
            Err(e) => return Err(e.into()),
        };
        self.swap_state(state);
        Ok(())
    }

    /// Swap the underlying state of the catalog.
    fn swap_state(&mut self, new_state: Arc<CatalogState>) {
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
