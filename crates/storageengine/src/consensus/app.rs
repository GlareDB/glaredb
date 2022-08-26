use super::{GlareNodeId, GlareRaft};

pub struct ApplicationState {
    pub id: GlareNodeId,
    pub raft: GlareRaft,
    pub api_addr: String,
    pub rpc_addr: String,
}
