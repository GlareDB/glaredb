use crate::repr::{NodeId, Raft};

pub struct ApplicationState {
    pub id: NodeId,
    pub raft: Raft,
    pub address: String,
}
