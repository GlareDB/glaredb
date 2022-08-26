use std::sync::Arc;

use openraft::{Raft as OpenRaft};

use crate::{network::ConsensusNetwork, store::ConsensusStore, message::{Request, Response}};

pub type NodeId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default, PartialOrd, Ord, Hash)]
pub struct Node {
    pub rpc_addr: String,
    pub api_addr: String,
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node {{ }}")
    }
}

pub type Raft = OpenRaft<RaftTypeConfig, Arc<ConsensusNetwork>, Arc<ConsensusStore>>;

openraft::declare_raft_types!(
    pub RaftTypeConfig: D = Request, R = Response, NodeId = NodeId, Node = Node
);

