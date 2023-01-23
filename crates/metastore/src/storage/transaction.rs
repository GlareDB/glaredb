use crate::storage::lease::{RemoteLease, RemoteLeaser};
use crate::storage::{Result, StorageError};
use crate::types::catalog::CatalogState;
use crate::types::storage::PersistedCatalog;
use object_store::ObjectStore;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Storage {
    store: Arc<dyn ObjectStore>,
    leaser: RemoteLeaser,
}

impl Storage {
    /// Read the state of some catalog.
    ///
    /// The catalog must already exist.
    pub async fn read_catalog(&self, db: Uuid) -> Result<PersistedCatalog> {
        unimplemented!()
    }
}

pub struct StorageTransaction<K, V> {
    /// The state at the beginning of the transaction.
    begin: HashMap<K, V>,
    /// Objects to be written to object storage.
    additions: HashMap<K, V>,
    /// Deletions to be made.
    deletions: HashSet<K>,
}

impl<K: Hash + Eq + Into<String>, V> StorageTransaction<K, V>
where
    K: Hash + Eq + Into<String>,
{
    /// Begin a transaction starting at some state.
    pub fn begin(state: HashMap<K, V>) -> Self {
        StorageTransaction {
            begin: state,
            additions: HashMap::new(),
            deletions: HashSet::new(),
        }
    }

    /// Get a value for some key.
    pub fn get(&self, key: &K) -> Option<&V> {
        if self.deletions.contains(key) {
            return None;
        }
        if let Some(v) = self.additions.get(key) {
            return Some(v);
        }
        self.begin.get(key)
    }

    /// Inserts a value for the transaction.
    ///
    /// Errors on duplicate keys.
    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        self.deletions.remove(&key);

        if self.begin.contains_key(&key) || self.additions.contains_key(&key) {
            return Err(StorageError::DuplicateKey(key.into()));
        }

        self.additions.insert(key, value);
        Ok(())
    }

    /// Update or insert a key with a given value.
    pub fn upsert(&mut self, key: K, value: V) {
        self.deletions.remove(&key);
        self.additions.insert(key, value);
    }

    /// Delete a key.
    pub fn delete(&mut self, key: K) {
        self.deletions.insert(key);
    }
}
