use crate::errors::Result;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

/// An all-or-nothing transcation against object storage.
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
        unimplemented!()
    }

    /// Update a key with a given value.
    pub fn update(&mut self, key: K, value: V) -> Result<()> {
        unimplemented!()
    }

    /// Delete a key.
    pub fn delete(&mut self, key: K) {
        unimplemented!()
    }
}
