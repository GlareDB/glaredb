use crate::repr::{Node, NodeId, RaftTypeConfig};

pub type AddLearnerResponse = openraft::raft::AddLearnerResponse<NodeId>;
pub type AddLearnerError = openraft::error::AddLearnerError<NodeId, Node>;

pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, Node>;

pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, Node>;
pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<RaftTypeConfig>;

pub type Infallible = openraft::error::Infallible;

pub type InitializeError = openraft::error::InitializeError<NodeId, Node>;

pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, Node>;

pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<RaftTypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<NodeId>;
pub type AppendEntriesError = openraft::error::AppendEntriesError<NodeId>;

pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<RaftTypeConfig>;
pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<NodeId>;
pub type InstallSnapshotError = openraft::error::InstallSnapshotError<NodeId>;

pub type Vote = openraft::Vote<NodeId>;
pub type VoteRequest = openraft::raft::VoteRequest<NodeId>;
pub type VoteResponse = openraft::raft::VoteResponse<NodeId>;
pub type VoteError = openraft::error::VoteError<NodeId>;

pub type Snapshot<T> = openraft::storage::Snapshot<NodeId, Node, T>;
pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, Node>;
pub type StorageError = openraft::StorageError<NodeId>;

pub type EffectiveMembership = openraft::EffectiveMembership<NodeId, Node>;

pub type LogId = openraft::LogId<NodeId>;

pub type StateMachineChanges = openraft::StateMachineChanges<RaftTypeConfig>;

pub type RaftMetrics = openraft::RaftMetrics<NodeId, Node>;
