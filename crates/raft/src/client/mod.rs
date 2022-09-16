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

        let mut n_retry = self.num_retries;

        loop {
            let res = self.write_rpc(&req).await;

            let rpc_err = match res {
                Ok(res) => return Ok(res),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let forward_err_res = <ClientWriteError as TryInto<OForwardToLeader>>::try_into(
                    remote_err.source.clone(),
                );

                if let Ok(ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                }) = forward_err_res
                {
                    // Update target to the "new" leader
                    self.update_leader(leader_id, leader_node).await.expect("failed to update leader");

                    n_retry -= 1;
                    if n_retry > 0 {
                        continue;
                    }
                }
            }

            return Err(rpc_err);
        }
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
    /// This method MUST return consitent value or CheckIsLeaderError.
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
    /// and ake the new node a member with [`change_membership`].
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
    ) -> RpcResult<OAddLearnerResponse, AddLearnerError> {
        let mut n_retry = self.num_retries;

        loop {
            let resp = self.add_learner_rpc(&req).await?;
            let res: RpcResult<OAddLearnerResponse, OAddLearnerError> =
                bincode::deserialize(&resp.payload).expect("failed to deserialize");

            let rpc_err = match res {
                Ok(res) => return Ok(res),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let forward_err_res = <AddLearnerError as TryInto<OForwardToLeader>>::try_into(
                    remote_err.source.clone(),
                );

                if let Ok(ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                }) = forward_err_res
                {
                    // Update target to the "new" leader
                    self.update_leader(leader_id, leader_node).await.expect("failed to update leader");

                    n_retry -= 1;
                    if n_retry > 0 {
                        continue;
                    }
                }
            }

            return Err(rpc_err);
        }
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
        let mut n_retry = self.num_retries;

        let req = ChangeMembershipRequest {
            payload: bincode::serialize(req).expect("failed to serialize"),
        };

        loop {
            let res = self.change_membership_rpc(&req).await;

            let rpc_err = match res {
                Ok(res) => return Ok(res),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let forward_err_res = <ClientWriteError as TryInto<OForwardToLeader>>::try_into(
                    remote_err.source.clone(),
                );

                if let Ok(ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                }) = forward_err_res
                {
                    self.update_leader(leader_id, leader_node).await.expect("failed to update leader");

                    n_retry -= 1;
                    if n_retry > 0 {
                        continue;
                    }
                }
            }

            return Err(rpc_err);
        }
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
