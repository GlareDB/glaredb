use std::sync::Arc;

use messaging::{GlareRequest, GlareResponse};
use network::{GlareNetworkConnection, ConsensusNetwork};
use openraft::{BasicNode, Raft};
use crate::rocks::RocksStore;

pub mod app;
pub mod error;
pub mod messaging;
pub mod network;
pub mod raft;
pub mod store;

pub type GlareNodeId = u64;

type GlareNode = BasicNode;
/*
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default, PartialOrd, Ord, Hash)]
pub struct GlareNode { 
    pub rpc_addr: String,
}

impl std::fmt::Display for GlareNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "GlareNode {{ }}")
    }
}
*/

pub type GlareRaft = Raft<GlareTypeConfig, Arc<ConsensusNetwork>, Arc<RocksStore>>;

openraft::declare_raft_types!(
    pub GlareTypeConfig: D = GlareRequest, R = GlareResponse, NodeId = GlareNodeId, Node = GlareNode
);
