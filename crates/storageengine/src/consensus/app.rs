use super::{GlareNodeId, GlareRaft};

pub struct ApplicationState {
    pub id: GlareNodeId,
    pub raft: GlareRaft,
}
