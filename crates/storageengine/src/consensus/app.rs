use super::{GlareNodeId, GlareRaft, network::ConsensusNetwork};

pub struct ApplicationState {
    pub id: GlareNodeId,
    pub raft: GlareRaft,
}
