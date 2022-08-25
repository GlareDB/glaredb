use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};

use openraft::{
    error::{AddLearnerError, CheckIsLeaderError, ClientWriteError, Infallible, InitializeError, ForwardToLeader, RPCError, NetworkError, RemoteError},
    raft::{AddLearnerResponse, ClientWriteResponse},
    RaftMetrics,
};
use serde::{Serialize, de::DeserializeOwned, Deserialize};
use tokio::sync::Mutex;

use super::{error::RpcResult, messaging::GlareRequest, GlareNode, GlareNodeId, GlareTypeConfig};
use crate::consensus::error::Result;

pub struct ConsensusClient {
    pub leader: Arc<Mutex<(GlareNodeId, SocketAddr)>>,
    // pub inner: toy_rpc::Client<AckModeNone>,
    pub inner: reqwest::Client,
}

// TODO: Implement functions

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Empty;

impl ConsensusClient {
    pub async fn new(leader_id: GlareNodeId, leader_addr: SocketAddr) -> Result<Self> {
        // let inner = toy_rpc::Client::dial(leader_addr).await?;
        Ok(Self {
            leader: Arc::new(Mutex::new((leader_id, leader_addr))),
            inner: reqwest::Client::new(),
        })
    }
    // --- Application API

    /// Submit a write request to the raft cluster
    ///
    /// the request will be processed by the raft protocol:
    /// it will be replicated to a quorum and then applied to the state machine.
    pub async fn write(
        &self,
        req: &GlareRequest,
    ) -> RpcResult<ClientWriteResponse<GlareTypeConfig>, ClientWriteError<GlareNodeId, GlareNode>>
    {
        self.send_rpc_to_leader("api/write", Some(req)).await
    }

    /// Read value by key
    ///
    /// this method may return a stale value as it does not force the read to be on a leader
    pub async fn read(&self, req: &str) -> RpcResult<String, Infallible> {
        self.send_rpc_to_leader("api/read", Some(&req)).await
    }

    /// Consistent read value by key
    ///
    /// this method MUST return a consistent value of CheckIsLeaderError
    pub async fn consistent_read(
        &self,
        req: &str,
    ) -> RpcResult<String, CheckIsLeaderError<GlareNodeId, GlareNode>> {
        self.send_rpc_to_leader("api/consistent_read", Some(req)).await
    }

    // --- Cluster management API

    /// Initialize a cluster starting with this node
    ///
    /// From the newly initialized cluster you may:
    /// - add a new node using [`write`].
    /// - set up replication with [`add_learner`].
    /// - make a learner node into a member with [`change_membership`].
    pub async fn init(&self) -> RpcResult<(), InitializeError<GlareNodeId, GlareNode>> {
        self.send_rpc_to_leader("cluster/init", Some(&Empty {})).await
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

    async fn do_send_rpc_to_leader<Req, Response, Err>(
        &self,
        path: &str,
        req: Option<&Req>,
    ) -> RpcResult<Response, Err> 
        where
            Req: Serialize + 'static,
            Response: Serialize + DeserializeOwned,
            Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let (leader_id, url) = {
            let t = self.leader.lock().await;
            (
                t.0,
                format!("http://{}/{}", t.1, path),
            )
        };

        // self.client
        let resp = if let Some(data) = req {
            let json = serde_json::to_string(&data).unwrap();
            println!(
                ">>> client send request to {}: {}",
                url,
                json
            );
            self.inner.post(url.clone()).json(data)
        } else {
            println!(">>> client send request to {}", url);
            self.inner.get(url.clone())
        }
        .send()
        .await
        .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<Response, Err> = resp.json().await.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        println!(
            "<<< client receive response from {}: {}",
            url,
            serde_json::to_string_pretty(&res).unwrap()
        );

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(leader_id, e)))
    }

    async fn send_rpc_to_leader<Req, Response, Err>(
        &self,
        path: &str,
        req: Option<&Req>,
    ) -> RpcResult<Response, Err>
        where
            Req: Serialize + 'static,
            Response: Serialize + DeserializeOwned,
            Err: std::error::Error
                + Serialize
                + DeserializeOwned
                + TryInto<ForwardToLeader<GlareNodeId, GlareNode>>
                + Clone,
    {
        // retry at most n times to find a valid leader
        let mut n_retry = 3;

        loop {
            let res: RpcResult<Response, Err> = 
                self.do_send_rpc_to_leader(path, req).await;

            let rpc_err = match res {
                Ok(r) => return Ok(r),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let forward_error_res = 
                    <Err as TryInto<ForwardToLeader<GlareNodeId, GlareNode>>>::try_into(remote_err.source.clone());

                if let Ok(ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                }) = forward_error_res {
                    // update target to be the new leader
                    {
                        let mut t = self.leader.lock().await;
                        let api_addr = leader_node.api_addr.clone();
                        *t = (leader_id, api_addr.parse::<SocketAddr>().unwrap());
                    }

                    n_retry -= 1;
                    if n_retry > 0 {
                        continue;
                    }
                }
            }

            return Err(rpc_err);
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientRequest {
    Init,
    AddLearner(GlareNodeId, SocketAddr),
    ChangeMembership(BTreeSet<GlareNodeId>),
    Metrics,
    Write(GlareRequest),
    Read(String),
    ConsistentRead(String),
}
