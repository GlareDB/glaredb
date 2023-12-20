use std::{
    borrow::Borrow,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;

/// Key used for the connections map.
// TODO: Possibly per user connections?
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ConnKey {
    pub(crate) ip: String,
    pub(crate) port: String,
}

pub struct ExpiringDashMap<K, V> {
    map: Arc<DashMap<K, (V, Instant)>>,
    ttl: Duration,
}

impl<K, V> ExpiringDashMap<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Create a new map with a default TTL of 5 minutes
    pub fn new() -> ExpiringDashMap<K, V> {
        Self::with_ttl(Duration::from_secs(300))
    }

    /// Create a new map with a custom TTL.
    /// This is a wrapper around [`DashMap`], but with automatic expiration of entries
    ///
    /// The TTL is the amount of time an entry will remain in the map after it was last accessed
    /// If an entry is not accessed for longer than the TTL, it will be removed from the map
    /// Every time an entry is accessed, the TTL is reset
    ///
    /// The entries are checked every minute
    pub fn with_ttl(ttl: Duration) -> ExpiringDashMap<K, V> {
        let map: Arc<DashMap<K, (V, Instant)>> = Arc::new(DashMap::new());
        let expiring_map = ExpiringDashMap { map, ttl };
        let weak_map = Arc::downgrade(&expiring_map.map);
        // Spawn a background task that will check for expired entries every minute
        // We don't want to run this loop forever
        // So we check if the map is still alive, and if it isn't, we break out of the loop
        // This will prevent the task from running forever
        tokio::spawn(async move {
            let ttl = expiring_map.ttl;
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                if let Some(map) = weak_map.upgrade() {
                    let now = Instant::now();
                    map.retain(|_, v| now - v.1 < ttl);
                } else {
                    break;
                }
            }
        });

        expiring_map
    }

    pub fn insert(&self, key: K, value: V) {
        self.map.insert(key, (value, Instant::now() + self.ttl));
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.map.get_mut(key).and_then(|mut entry| {
            if entry.value().1 > Instant::now() {
                entry.value_mut().1 = Instant::now() + self.ttl;
                Some(entry.value().0.clone())
            } else {
                self.map.remove(key);
                None
            }
        })
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: core::hash::Hash + Eq + ?Sized,
    {
        self.map.contains_key(key)
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: core::hash::Hash + Eq + ?Sized,
    {
        self.map.remove(key).map(|(k, v)| (k, v.0))
    }
}
