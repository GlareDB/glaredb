use super::NodeId;
use super::{AccordError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

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

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Address::Peer(node) => write!(f, "{}", node),
            Address::Peers => write!(f, "all"),
            Address::Local => write!(f, "local"),
        }
    }
}

pub type TopologyManagerRef = Arc<TopologyManager>;

#[derive(Debug)]
pub struct TopologyManager {
    current: Topology,
}

impl TopologyManager {
    pub fn new(initial: Topology) -> TopologyManager {
        TopologyManager { current: initial }
    }

    pub fn get_current(&self) -> &Topology {
        &self.current
    }
}

#[derive(Debug)]
pub struct QuorumCheck {
    pub have_fast_path: bool,
    pub have_slow_path: bool,
}

/// Single-shard topology.
#[derive(Debug)]
pub struct Topology {
    /// All replicas in the shard, with their socket addresses.
    peers: HashMap<NodeId, SocketAddr>,
    /// Fast path electorate. Must be a subset of `peers`.
    electorate: HashMap<NodeId, SocketAddr>,
}

impl Topology {
    /// Create a new topology with the given peers and electorate. The
    /// electorate must be a subset of peers.
    pub fn new<I>(peers: I, electorate: I) -> Result<Topology>
    where
        I: IntoIterator<Item = (NodeId, SocketAddr)>,
    {
        let peers: HashMap<_, _> = peers.into_iter().collect();
        let electorate: HashMap<_, _> = electorate.into_iter().collect();

        if electorate.len() > peers.len() {
            return Err(AccordError::ElectorateNotSubset);
        }
        for (id, addr) in electorate.iter() {
            match peers.get(id) {
                Some(check_addr) if check_addr == addr => (),
                _ => return Err(AccordError::ElectorateNotSubset),
            }
        }

        if electorate.len() < slow_path_quorum_size(peers.len()) {
            return Err(AccordError::InvalidElectorateSize(electorate.len()));
        }

        Ok(Topology { peers, electorate })
    }

    pub fn iter_electorate_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.electorate.iter().map(|(id, _)| id)
    }

    pub fn iter_peer_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.peers.iter().map(|(id, _)| id)
    }

    pub fn iter_peers(&self) -> impl Iterator<Item = (&NodeId, &SocketAddr)> {
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

    fn fake_addr(id: NodeId) -> (NodeId, SocketAddr) {
        let port = id % u16::MAX as NodeId;
        (id, format!("127.0.0.1:{}", port).parse().unwrap())
    }

    #[test]
    fn electorate_not_subset() {
        let _ = Topology::new(
            vec![fake_addr(1), fake_addr(2), fake_addr(3)],
            vec![fake_addr(3), fake_addr(4)],
        )
        .expect_err("not a subset");
    }

    #[test]
    fn quorum() {
        let topology = Topology::new(
            vec![
                fake_addr(1),
                fake_addr(2),
                fake_addr(3),
                fake_addr(4),
                fake_addr(5),
            ],
            vec![fake_addr(1), fake_addr(2), fake_addr(3)],
        )
        .unwrap();

        let check = topology.check_quorum(&[1, 2]);
        assert!(!check.have_fast_path);
        assert!(!check.have_slow_path);

        let check = topology.check_quorum(&[1, 2, 3]);
        assert!(check.have_fast_path);
        assert!(check.have_slow_path);

        let topology = Topology::new(
            vec![
                fake_addr(1),
                fake_addr(2),
                fake_addr(3),
                fake_addr(4),
                fake_addr(5),
            ],
            vec![
                fake_addr(1),
                fake_addr(2),
                fake_addr(3),
                fake_addr(4),
                fake_addr(5),
            ],
        )
        .unwrap();

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

    #[test]
    fn single_peer() {
        let topology = Topology::new(vec![fake_addr(1)], vec![fake_addr(1)]).unwrap();
        let check = topology.check_quorum(&[1]);
        assert!(check.have_fast_path);
        assert!(check.have_slow_path);
    }
}
