use std::{collections::BTreeSet, sync::Arc};

use openraft::error::{ForwardToLeader, NetworkError, RPCError};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tonic::transport::Endpoint;

use super::error::RpcResult;
use crate::message::{ReadTxRequest, ReadTxResponse, Request};
use crate::openraft_types::prelude::*;
use crate::repr::Node;
use crate::rpc::pb::raft_node_client::RaftNodeClient;
use crate::rpc::pb::remote_data_source_client::RemoteDataSourceClient;
use crate::rpc::pb::{
    AddLearnerRequest, AddLearnerResponse, BinaryReadRequest, BinaryWriteRequest,
    ChangeMembershipRequest,
};
use crate::{
    error::RpcError,
    openraft_types::types::{
        AddLearnerError, CheckIsLeaderError, ClientWriteError, ClientWriteResponse, Infallible,
        InitializeError, RaftMetrics,
    },
    repr::NodeId,
};

#[derive(Debug, Clone)]
pub struct NodeClients {
    pub app_client: RemoteDataSourceClient<tonic::transport::Channel>,
    pub node_client: RaftNodeClient<tonic::transport::Channel>,
}

impl NodeClients {
    pub async fn from_endpoint(
        endpoint: Endpoint,
    ) -> Result<Self, tonic::transport::Error> {
        let app_client = RemoteDataSourceClient::connect(endpoint.clone()).await?;
        let node_client = RaftNodeClient::connect(endpoint).await?;

        Ok(Self {
            app_client,
            node_client,
        })
    }
}

pub struct ConsensusClient {
    pub leader: Arc<Mutex<(NodeId, NodeClients)>>,
    pub num_retries: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Empty;

const DEFAULT_NUM_RETRIES: usize = 3;

/// Perform an rpc caller to the leader node.
/// If the contact node is not the leader, the request will be forwarded to the leader.
/// The request will be retried if the leader is not available, up to the client's `num_retries`.
macro_rules! retry_rpc_on_leader {
    ($client:ident, $func:ident, $req:ident, $errtype:ident) => {
        {
            let mut n_retry = $client.num_retries;

            loop {
                let res = $client.$func(&$req).await;

                // If the request is successful, return the result.
                let rpc_err = match res {
                    Ok(res) => break Ok(res),
                    Err(rpc_err) => rpc_err,
                };

                // If the request is not successful, check if the error is a `ForwardToLeader` error.
                if let RPCError::RemoteError(remote_err) = &rpc_err {
                    let forward_err_res = <$errtype as TryInto<OForwardToLeader>>::try_into(
                        remote_err.source.clone(),
                    );

                    // If the error is a `ForwardToLeader` error, update the stored leader and retry.
                    if let Ok(ForwardToLeader {
                        leader_id: Some(leader_id),
                        leader_node: Some(leader_node),
                    }) = forward_err_res
                    {
                        $client.update_leader(leader_id, leader_node).await.expect("failed to update leader");

                        n_retry -= 1;
                        if n_retry > 0 {
                            continue;
                        }
                    }
                }

                // If we reach here, we have exhausted all retries.
                break Err(rpc_err);
            }
        }
    }
}

impl ConsensusClient {
    /// Create a client with a leader node id and a node manager to get node address by node id.
    pub async fn new(leader_id: NodeId, leader_url: String) -> RpcResult<Self, NetworkError> {
        let endpoint = Endpoint::from_shared(leader_url).expect("failed to create endpoint");

        let clients = NodeClients::from_endpoint(endpoint).await.expect("failed to create clients to leader");

        Ok(Self {
            leader: Arc::new(Mutex::new((leader_id, clients))),
            num_retries: DEFAULT_NUM_RETRIES,
        })
    }

    // --- Application API

    async fn write_rpc(
        &self,
        req: &BinaryWriteRequest,
    ) -> RpcResult<ClientWriteResponse, ClientWriteError> {
        let (_leader_id, mut clients) = self.leader.lock().await.clone();

        let resp = clients
            .app_client
            .write(req.clone())
            .await
            .map(|resp| {
                bincode::deserialize(&resp.into_inner().payload).expect("failed to deserialize")
            })
            .map_err(|e| RpcError::Network(NetworkError::new(&e)))?;

        Ok(resp)
    }

    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then will be applied to
    /// state machine.
    ///
    /// The result of applying the request will be returned.
    pub async fn write(&self, req: Request) -> RpcResult<ClientWriteResponse, ClientWriteError> {
        let req: BinaryWriteRequest = req.into();

        retry_rpc_on_leader!(self, write_rpc, req, ClientWriteError)
    }

    async fn read_rpc(&self, req: &BinaryReadRequest) -> RpcResult<ReadTxResponse, Infallible> {
        let (_leader_id, mut clients) = self.leader.lock().await.clone();

        let resp = clients
            .app_client
            .read(req.clone())
            .await
            .map(|resp| {
                bincode::deserialize(&resp.into_inner().payload).expect("failed to deserialize")
            })
            .map_err(|e| RpcError::Network(NetworkError::new(&e)))?;

        Ok(resp)
    }

    /// Read value by key, in an inconsistent mode.
    ///
    /// This method may return stale value because it does not force to read on a legal leader.
    pub async fn read(&self, req: ReadTxRequest) -> RpcResult<ReadTxResponse, Infallible> {
        let req: BinaryReadRequest = req.into();

        self.read_rpc(&req).await
    }

    /// Consistent Read value by key, in an inconsistent mode.
    ///
    /// This method MUST return consistent value or CheckIsLeaderError.
    pub async fn consistent_read(
        &self,
        _req: ReadTxRequest,
    ) -> RpcResult<ReadTxResponse, CheckIsLeaderError> {
        todo!();
    }

    // --- Cluster management API

    /// Initialize a cluster of only the node that receives this request.
    ///
    /// This is the first step to initialize a cluster.
    /// With a initialized cluster, new nodes can be added with [`write`].
    /// Then setup replication with [`add_learner`].
    /// and make the new node a member with [`change_membership`].
    pub async fn init(&self) -> RpcResult<(), InitializeError> {
        let (_leader_id, mut clients) = self.leader.lock().await.clone();

        clients
            .node_client
            .init(())
            .await
            .map_err(|e| RpcError::Network(NetworkError::new(&e)))?;

        Ok(())
    }

    async fn add_learner_rpc(
        &self,
        req: &AddLearnerRequest,
    ) -> RpcResult<AddLearnerResponse, AddLearnerError> {
        let (_leader_id, mut clients) = self.leader.lock().await.clone();

        let resp = clients
            .node_client
            .add_learner(req.clone())
            .await
            .map_err(|e| RpcError::Network(NetworkError::new(&e)))?;

        Ok(resp.into_inner())
    }

    /// Add a node as learner.
    ///
    /// The node to add has to exist, i.e., being added with `write(Request::AddNode{})`
    pub async fn add_learner(
        &self,
        req: AddLearnerRequest,
    ) -> RpcResult<AddLearnerResponse, AddLearnerError> {
        retry_rpc_on_leader!(self, add_learner_rpc, req, AddLearnerError)
    }

    async fn change_membership_rpc(
        &self,
        req: &ChangeMembershipRequest,
    ) -> RpcResult<OClientWriteResponse, OClientWriteError> {
        let (_leader_id, mut client) = self.leader.lock().await.clone();

        let resp = client
            .node_client
            .change_membership(req.clone())
            .await
            .map(|resp| {
                bincode::deserialize(&resp.into_inner().payload).expect("failed to deserialize")
            })
            .map_err(|e| RpcError::Network(NetworkError::new(&e)))?;

        Ok(resp)
    }

    /// Change membership to the specified set of nodes.
    ///
    /// All nodes in `req` have to be already added as learner with [`add_learner`],
    /// or an error [`LearnerNotFound`] will be returned.
    pub async fn change_membership(
        &self,
        req: &BTreeSet<NodeId>,
    ) -> RpcResult<OClientWriteResponse, OClientWriteError> {

        let req = ChangeMembershipRequest {
            payload: bincode::serialize(req).expect("failed to serialize"),
        };

        retry_rpc_on_leader!(self, change_membership_rpc, req, OClientWriteError)
    }

    /// Get the metrics about the cluster.
    ///
    /// Metrics contains various information about the cluster, such as current leader,
    /// membership config, replication status etc.
    /// See [`RaftMetrics`].
    pub async fn metrics(&self) -> RpcResult<RaftMetrics, Infallible> {
        let (_leader_id, mut client) = self.leader.lock().await.clone();

        match client
            .node_client
            .metrics(()).await {
            Ok(resp) => Ok(resp.into_inner().try_into().unwrap()),
            Err(e) => Err(RpcError::Network(NetworkError::new(&e))),
        }
    }

    pub async fn update_leader(&self, id: NodeId, node: Node) -> Result<(), tonic::transport::Error> {
        let mut t = self.leader.lock().await;
        let url = node.address.clone();
        let endpoint =
            Endpoint::from_shared(url).expect("failed to create endpoint");
        let clients = NodeClients::from_endpoint(endpoint).await?;

        *t = (id, clients);

        Ok(())
    }

}
