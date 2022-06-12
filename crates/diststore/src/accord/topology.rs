use super::NodeId;
use super::{AccordError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

const MIN_NUM_PEERS: usize = 3;

const fn max_tolerator_failures(peers: usize) -> usize {
    (peers - 1) / 2
}

const fn fast_path_quorum_size(peers: usize, electorate: usize) -> usize {
    let f = max_tolerator_failures(peers);
    (electorate + f) / 2 + 1
}

const fn slow_path_quorum_size(peers: usize) -> usize {
    peers - max_tolerator_failures(peers)
}

/// Where to send messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Address {
    /// Broadcast to specific peer.
    Peer(NodeId),
    /// Broadcast to all peers.
    Peers,
    /// Broadcast only to this node.
    Local,
}

#[derive(Debug)]
pub struct QuorumCheck {
    pub have_fast_path: bool,
    pub have_slow_path: bool,
}

/// Single-shard topology.
#[derive(Debug)]
pub struct Topology {
    /// All replicas in the shard.
    peers: HashSet<NodeId>,
    /// Fast path electorate. Must be a subset of `peers`.
    electorate: HashSet<NodeId>,
}

impl Topology {
    /// Create a new topology with the given peers and electorate. The
    /// electorate must be a subset of peers.
    pub fn new<I>(peers: I, electorate: I) -> Result<Topology>
    where
        I: IntoIterator<Item = NodeId>,
    {
        let peers: HashSet<_> = peers.into_iter().collect();
        let electorate: HashSet<_> = electorate.into_iter().collect();

        if peers.len() < MIN_NUM_PEERS {
            return Err(AccordError::NotEnoughPeers);
        }

        if !electorate.is_subset(&peers) {
            return Err(AccordError::ElectorateNotSubset);
        }

        if electorate.len() < slow_path_quorum_size(peers.len()) {
            return Err(AccordError::InvalidElectorateSize(electorate.len()));
        }

        Ok(Topology { peers, electorate })
    }

    pub fn get_electorate(&self) -> impl Iterator<Item = &NodeId> {
        self.electorate.iter()
    }

    pub fn get_peers(&self) -> impl Iterator<Item = &NodeId> {
        self.peers.iter()
    }

    pub fn check_quorum<'a, I>(&self, nodes: I) -> QuorumCheck
    where
        I: IntoIterator<Item = &'a NodeId>,
    {
        let mut fp_count = 0;
        let mut sp_count = 0;

        for node in nodes.into_iter() {
            if self.electorate.get(node).is_some() {
                fp_count += 1;
            }
            // Assume nodes passed in are actually part of the group.
            sp_count += 1;
        }

        let fp_size = fast_path_quorum_size(self.peers.len(), self.electorate.len());
        let sp_size = slow_path_quorum_size(self.peers.len());

        QuorumCheck {
            have_fast_path: fp_count >= fp_size,
            have_slow_path: sp_count >= sp_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum() {
        let topology = Topology::new(vec![1, 2, 3, 4, 5], vec![1, 2, 3]).unwrap();

        let check = topology.check_quorum(&[1, 2]);
        assert!(!check.have_fast_path);
        assert!(!check.have_slow_path);

        let check = topology.check_quorum(&[1, 2, 3]);
        assert!(check.have_fast_path);
        assert!(check.have_slow_path);

        let topology = Topology::new(vec![1, 2, 3, 4, 5], vec![1, 2, 3, 4, 5]).unwrap();

        let check = topology.check_quorum(&[1, 2]);
        assert!(!check.have_fast_path);
        assert!(!check.have_slow_path);

        let check = topology.check_quorum(&[1, 2, 3]);
        assert!(!check.have_fast_path);
        assert!(check.have_slow_path);

        let check = topology.check_quorum(&[1, 2, 3, 4]);
        assert!(check.have_fast_path);
        assert!(check.have_slow_path);
    }
}
