use super::repr::{NodeId, Raft};

pub struct ApplicationState {
    pub id: NodeId,
    pub raft: Raft,
    pub api_addr: String,
    pub rpc_addr: String,
}
