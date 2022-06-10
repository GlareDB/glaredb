use serde::{Deserialize, Serialize};
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fmt;
use std::sync::atomic::{self, AtomicU64};
use std::time::SystemTime;

use super::NodeId;

/// Timestamps for identifying transactions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct Timestamp {
    /// Unix time in milliseconds.
    ///
    /// Only lower 48 bits are used to help with atomically generating unique
    /// timestamps in `TimestampProvider`. 48 bits is more than enough to store
    /// the current date (up to year ~10,000).
    unix_millis: u64,
    logical: u16,
    node: NodeId,
}

impl Timestamp {
    /// Create a timestamp from the provided parts.
    pub fn new(unix_millis: u64, logical: u16, node: NodeId) -> Timestamp {
        debug_assert!(
            unix_millis < (1 << 48) - 1,
            "unix timestamp greater than u48"
        );
        Timestamp {
            unix_millis,
            logical,
            node,
        }
    }

    /// Create a new timestamp for a given node.
    pub fn now(node: NodeId) -> Timestamp {
        // 1. Millisecond precision can safely be fit into a u64.
        // 2. Will not panic unless the system time is set to before unix epoch.
        // TODO: Benchmark to make sure this isn't slow.
        let unix_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("expected system time to be after unix epoch")
            .as_millis() as u64;

        Timestamp {
            unix_millis,
            logical: 0,
            node,
        }
    }

    /// Get the next logical timestamp using the provided node id.
    pub fn next_logical(&self, node: NodeId) -> Timestamp {
        Timestamp {
            unix_millis: self.unix_millis,
            logical: self.logical + 1,
            node,
        }
    }
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let cmp = self.unix_millis.cmp(&other.unix_millis);
        let cmp = if cmp.is_eq() {
            self.logical.cmp(&other.logical)
        } else {
            cmp
        };
        let cmp = if cmp.is_eq() {
            self.node.cmp(&other.node)
        } else {
            cmp
        };
        Some(cmp)
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {}, {})", self.unix_millis, self.logical, self.node)
    }
}

/// A thread-safe timestamp provider.
///
/// All generated timestamps on a single node will be unique. Given `Timestamp`
/// has millisecond precision, timestamps generated within the same millisecond
/// will have their logical parts differ.
#[derive(Debug)]
pub struct TimestampProvider {
    node: NodeId,
    /// Track both the unix timestamp and logical timestamp.
    ///
    /// Lower 48 bits is unix timestamp, upper 16 is logical.
    millis_and_logical: AtomicU64,
}

impl TimestampProvider {
    pub fn new(node: NodeId) -> TimestampProvider {
        let now = Timestamp::now(node);
        TimestampProvider {
            node,
            millis_and_logical: AtomicU64::new(now.unix_millis), // Upper bits already zero.
        }
    }

    /// Return a unique "now" timestamp.
    pub fn unique_now(&self) -> Timestamp {
        let cand = Timestamp::now(self.node);
        let prev = self
            .millis_and_logical
            .fetch_update(atomic::Ordering::SeqCst, atomic::Ordering::SeqCst, |prev| {
                let mut millis = prev & (1 << 48) - 1;
                let mut logical = prev >> 48;
                if millis >= cand.unix_millis {
                    logical += 1;
                } else {
                    millis = cand.unix_millis;
                    logical = 0;
                }
                let logical = logical << 48;
                Some(millis | logical)
            })
            .unwrap(); // `None` never returned from closure.

        let millis = prev & (1 << 48) - 1;
        let logical = (prev >> 48) as u16;
        Timestamp::new(millis, logical, self.node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn provider_unique_now() {
        let runs = 200;
        let mut timestamps = Vec::with_capacity(200);
        let node = 23;

        let prov = TimestampProvider::new(node);

        println!("now: {:?}", Timestamp::now(node));

        // These will likely all operate within the same millisecond. It's fine
        // if some go beyond that, just need to test that the logical counter
        // works for some of them.
        for _i in 0..runs / 2 {
            timestamps.push(prov.unique_now());
        }

        // Ensure we get some timestamps that include a different millisecond
        // value.
        sleep(Duration::from_millis(1));
        for _i in 0..runs / 2 {
            timestamps.push(prov.unique_now());
        }

        println!("first timestamp: {:?}", timestamps.first().unwrap());
        println!("last timestamp: {:?}", timestamps.last().unwrap());

        let mut iter = timestamps.iter();
        let mut prev = iter.next().unwrap();
        iter.all(|curr| {
            assert!(prev < curr, "prev {:?} not less than curr {:?}", prev, curr);
            prev = curr;
            true
        });
    }

    #[test]
    fn cmp() {
        let tests = vec![
            (
                Timestamp::new(0, 0, 0),
                Timestamp::new(0, 0, 0),
                Ordering::Equal,
            ),
            (
                Timestamp::new(1, 0, 0),
                Timestamp::new(0, 0, 0),
                Ordering::Greater,
            ),
            (
                Timestamp::new(0, 0, 0),
                Timestamp::new(1, 0, 0),
                Ordering::Less,
            ),
            (
                Timestamp::new(0, 2, 2),
                Timestamp::new(0, 2, 2),
                Ordering::Equal,
            ),
            (
                Timestamp::new(0, 3, 2),
                Timestamp::new(0, 2, 2),
                Ordering::Greater,
            ),
            (
                Timestamp::new(0, 2, 2),
                Timestamp::new(0, 3, 2),
                Ordering::Less,
            ),
            (
                Timestamp::new(0, 0, 4),
                Timestamp::new(0, 0, 4),
                Ordering::Equal,
            ),
            (
                Timestamp::new(0, 0, 5),
                Timestamp::new(0, 0, 4),
                Ordering::Greater,
            ),
            (
                Timestamp::new(0, 0, 4),
                Timestamp::new(0, 0, 5),
                Ordering::Less,
            ),
        ];

        for (a, b, expected) in tests.into_iter() {
            assert_eq!(expected, a.cmp(&b), "a: {:?}, b: {:?}", a, b);
        }
    }

    #[test]
    fn next_logical_greater() {
        let now = Timestamp::now(1);
        let next = now.next_logical(1);
        assert!(next > now);
    }
}
