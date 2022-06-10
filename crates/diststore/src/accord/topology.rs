use super::NodeId;
use super::{AccordError, Result};
use std::collections::HashSet;

const TOLERATED_FAILURES: usize = 1;

const MIN_NUM_PEERS: usize = 3;

const fn fast_path_size(electorate_size: usize) -> usize {
    (electorate_size + TOLERATED_FAILURES + 1) / 2
}

/// Single-shard topology.
#[derive(Debug)]
pub struct Topology {
    /// All replicas in the shard.
    peers: HashSet<NodeId>,
    /// Voting electorate. Must be a subset of `peers`.
    electorate: HashSet<NodeId>,
}

impl Topology {
    /// Create a new topology where all peers can vote.
    ///
    /// There must be at least 3 peers.
    ///
    /// Locality of peers are not taken into account.
    pub fn new_all_voting<I>(peers: I) -> Result<Topology>
    where
        I: IntoIterator<Item = NodeId>,
    {
        let peers: HashSet<_> = peers.into_iter().collect();
        if peers.len() < MIN_NUM_PEERS {
            return Err(AccordError::NotEnoughPeers);
        }
        let electorate = peers.clone();
        Ok(Topology { peers, electorate })
    }
}
