use std::sync::Arc;

use crate::{repr::{NodeId, Raft}, store::ConsensusStore};

pub struct ApplicationState {
    pub id: NodeId,
    pub raft: Raft,
    pub address: String,
    pub store: Arc<ConsensusStore>,
}
