use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};

use openraft::{
    error::{AddLearnerError, CheckIsLeaderError, ClientWriteError, Infallible, InitializeError},
    raft::{AddLearnerResponse, ClientWriteResponse},
    RaftMetrics,
};
use tokio::sync::Mutex;
use toy_rpc::pubsub::AckModeNone;

use super::{error::RpcResult, messaging::GlareRequest, GlareNode, GlareNodeId, GlareTypeConfig};
use crate::consensus::error::Result;

pub struct ConsensusClient {
    pub leader: Arc<Mutex<(GlareNodeId, SocketAddr)>>,
    pub inner: toy_rpc::Client<AckModeNone>,
}

// TODO: Implement functions

impl ConsensusClient {
    pub async fn new(leader_id: GlareNodeId, leader_addr: SocketAddr) -> Result<Self> {
        let inner = toy_rpc::Client::dial(leader_addr).await?;
        Ok(Self {
            leader: Arc::new(Mutex::new((leader_id, leader_addr))),
            inner,
        })
    }
    // --- Application API

    /// Submit a write request to the raft cluster
    ///
    /// the request will be processed by the raft protocol:
    /// it will be replicated to a quorum and then applied to the state machine.
    pub async fn write(
        &self,
        _req: &GlareRequest,
    ) -> RpcResult<ClientWriteResponse<GlareTypeConfig>, ClientWriteError<GlareNodeId, GlareNode>>
    {
        todo!();
    }

    /// Read value by key
    ///
    /// this method may return a stale value as it does not force the read to be on a leader
    pub async fn read(&self, _req: &str) -> RpcResult<String, Infallible> {
        todo!();
    }

    /// Consistent read value by key
    ///
    /// this method MUST return a consistent value of CheckIsLeaderError
    pub async fn consistent_read(
        &self,
        _req: &str,
    ) -> RpcResult<String, CheckIsLeaderError<GlareNodeId, GlareNode>> {
        todo!();
    }

    // --- Cluster management API

    /// Initialize a cluster starting with this node
    ///
    /// From the newly initialized cluster you may:
    /// - add a new node using [`write`].
    /// - set up replication with [`add_learner`].
    /// - make a learner node into a member with [`change_membership`].
    pub async fn init(&self) -> Result<(), RpcResult<(), InitializeError<GlareNodeId, GlareNode>>> {
        todo!();
    }

    /// Add a node as a learner
    ///
    /// the node must already exist, having been creating with [`write`].
    pub async fn add_learner(
        &self,
        _req: (GlareNodeId, SocketAddr),
    ) -> RpcResult<AddLearnerResponse<GlareNodeId>, AddLearnerError<GlareNodeId, GlareNode>> {
        todo!();
    }

    pub async fn change_membership(
        &self,
        _req: &BTreeSet<GlareNodeId>,
    ) -> RpcResult<ClientWriteResponse<GlareTypeConfig>, ClientWriteError<GlareNodeId, GlareNode>>
    {
        todo!();
    }

    /// Get the metrics of the cluster
    ///
    /// metrics contains various information, such as:
    /// - the current leader
    /// - membership config
    /// - replication status
    pub async fn metrics(&self) -> RpcResult<RaftMetrics<GlareNodeId, GlareNode>, Infallible> {
        todo!();
    }
}
