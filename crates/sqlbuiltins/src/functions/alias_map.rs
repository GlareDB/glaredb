use std::borrow::Borrow;
use std::collections::{hash_map, HashMap};
use std::hash::Hash;
use std::iter::FromIterator;

/// A map allowing for multiple keys to point to the same value.
#[derive(Debug, Clone)]
pub struct AliasMap<K, V> {
    /// All values in the map.
    values: Vec<V>,

    /// Points keys to the index of the value.
    keys_to_index: HashMap<K, usize>,
}

impl<K, V> AliasMap<K, V> {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            keys_to_index: HashMap::new(),
        }
    }

    /// Return an iterator over all entries in the map.
    ///
    /// All aliases will be iterated over, and will result in a value being
    /// returned multiple times if it has multiple aliases.
    #[allow(dead_code)]
    pub fn iter(&self) -> AliasMapIter<K, V> {
        AliasMapIter {
            values: &self.values,
            key_iter: self.keys_to_index.iter(),
        }
    }

    /// Return an iterator over all values.
    ///
    /// Values will only be returned once, even if there's multiple keys
    /// pointing to it.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.values.iter()
    }
}

impl<K: Hash + Eq, V> AliasMap<K, V> {
    /// Get a value by key from the map.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let idx = self.keys_to_index.get(key)?;
        self.values.get(*idx)
    }

    /// Insert a value with all provided keys pointing to that value.
    pub fn insert_aliases(&mut self, keys: impl IntoIterator<Item = K>, value: V) {
        let idx = self.values.len();
        self.values.push(value);

        for key in keys {
            self.keys_to_index.insert(key, idx);
        }
    }
}

impl<K: Hash + Eq, V> FromIterator<(Vec<K>, V)> for AliasMap<K, V> {
    fn from_iter<T: IntoIterator<Item = (Vec<K>, V)>>(iter: T) -> Self {
        let mut m = AliasMap::new();
        for (keys, value) in iter {
            m.insert_aliases(keys, value);
        }
        m
    }
}

impl<K, V> Default for AliasMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over an alias map.
///
/// A value may be returned more than once if is has more than one alias.
#[derive(Debug)]
pub struct AliasMapIter<'a, K, V> {
    values: &'a Vec<V>,
    key_iter: hash_map::Iter<'a, K, usize>,
}

impl<'a, K, V> Iterator for AliasMapIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let (k, idx) = self.key_iter.next()?;
        let v = self.values.get(*idx).expect("value should exist");
        Some((k, v))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.key_iter.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert() {
        let mut m = AliasMap::new();
        m.insert_aliases(vec!["hello", "world"], 1);
        m.insert_aliases(vec!["mario", "luigi"], 2);
        m.insert_aliases(vec!["peach"], 3);

        assert_eq!(Some(&1), m.get(&"hello"));
        assert_eq!(Some(&1), m.get(&"world"));
        assert_eq!(Some(&2), m.get(&"mario"));
        assert_eq!(Some(&2), m.get(&"luigi"));
        assert_eq!(Some(&3), m.get(&"peach"));
        assert_eq!(None, m.get(&"bowser"));
    }

    #[test]
    fn from_iter() {
        let m = [
            (vec!["hello", "world"], 1),
            (vec!["mario", "luigi"], 2),
            (vec!["peach"], 3),
        ]
        .into_iter()
        .collect::<AliasMap<_, _>>();

        assert_eq!(Some(&1), m.get(&"hello"));
        assert_eq!(Some(&1), m.get(&"world"));
        assert_eq!(Some(&2), m.get(&"mario"));
        assert_eq!(Some(&2), m.get(&"luigi"));
        assert_eq!(Some(&3), m.get(&"peach"));
        assert_eq!(None, m.get(&"bowser"));
    }

    #[test]
    fn aliases_iter() {
        let m = [
            (vec!["hello", "world"], 1),
            (vec!["mario", "luigi"], 2),
            (vec!["peach"], 3),
        ]
        .into_iter()
        .collect::<AliasMap<_, _>>();

        let mut collected = m.iter().collect::<Vec<_>>();
        collected.sort(); // Lexicographical

        let expected = vec![
            (&"hello", &1),
            (&"luigi", &2),
            (&"mario", &2),
            (&"peach", &3),
            (&"world", &1),
        ];

        assert_eq!(expected, collected);
    }

    #[test]
    fn values_iter() {
        let m = [
            (vec!["hello", "world"], 1),
            (vec!["mario", "luigi"], 2),
            (vec!["peach"], 3),
        ]
        .into_iter()
        .collect::<AliasMap<_, _>>();

        let mut collected = m.values().collect::<Vec<_>>();
        collected.sort();

        assert_eq!(vec![&1, &2, &3], collected);
    }
}
