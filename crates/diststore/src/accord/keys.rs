//! Key-based conflict detection.
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;

/// A type that represents a key to either a single entry, or a range of entries
/// in the system. This key is used when detecting transaction conflicts.
pub trait Key:
    Serialize
    + DeserializeOwned
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
    + Hash
    + Debug
    + Clone
    + Send
    + Unpin
    + 'static
{
    /// Whether or not this key conflicts with another key.
    fn conflicts_with(&self, other: &Self) -> bool;
}

/// All keys involved in a transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeySet<K> {
    keys: Vec<K>,
}

impl<K: Key> KeySet<K> {
    pub fn from_keys<I>(keys: I) -> Self
    where
        I: IntoIterator<Item = K>,
    {
        KeySet {
            keys: keys.into_iter().collect(),
        }
    }

    pub fn from_key(key: K) -> Self {
        KeySet { keys: vec![key] }
    }

    /// Check if there's a conflict between this set and another key.
    pub fn conflicts_with_key(&self, key: &K) -> bool {
        self.keys.iter().any(|check| check.conflicts_with(key))
    }

    /// Check if this set's keys conflicts with any keys in another set.
    pub fn conflicts_with_any(&self, other: &Self) -> bool {
        // TODO: Keeping the keys sorted could be a decent perf improvement.
        self.keys
            .iter()
            .any(|check| other.conflicts_with_key(check))
    }
}

/// A simple implementation of `Key` on a string where conflicts only occur when
/// two strings equal each other.
#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Clone)]
pub struct ExactString(pub String);

impl Key for ExactString {
    fn conflicts_with(&self, other: &Self) -> bool {
        self == other
    }
}

impl From<&str> for ExactString {
    fn from(s: &str) -> Self {
        ExactString(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_conflicts() {
        let keys_a = vec!["a", "b", "c"].into_iter().map(ExactString::from);
        let set_a = KeySet::from_keys(keys_a);

        let keys_b = vec!["d", "e", "f"].into_iter().map(ExactString::from);
        let set_b = KeySet::from_keys(keys_b);

        assert!(!set_a.conflicts_with_any(&set_b));
        assert!(!set_b.conflicts_with_any(&set_a));
    }

    #[test]
    fn conflicts() {
        let keys_a = vec!["a", "b", "c"].into_iter().map(ExactString::from);
        let set_a = KeySet::from_keys(keys_a);

        let keys_b = vec!["d", "e", "b", "f"].into_iter().map(ExactString::from);
        let set_b = KeySet::from_keys(keys_b);

        assert!(set_a.conflicts_with_any(&set_b));
        assert!(set_b.conflicts_with_any(&set_a));
    }
}
