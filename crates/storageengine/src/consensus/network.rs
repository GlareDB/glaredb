use std::any::Any;
use std::sync::Arc;

use openraft::async_trait::async_trait;
use openraft::error::{
    AppendEntriesError, InstallSnapshotError, NetworkError, RPCError, RemoteError, VoteError,
};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{AnyError, RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use serde::Serialize;
use toy_rpc::pubsub::AckModeNone;

use super::error::{Error, RpcError, RpcResult};
use super::raft::RaftClientStub;
use super::{GlareNode, GlareNodeId, GlareTypeConfig};

pub struct ConsensusNetwork {}

impl ConsensusNetwork {
    pub async fn send_rpc<Req, Resp>() -> Result<Resp, RPCError<GlareNodeId, GlareNode, Error>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        todo!()
    }
}

#[async_trait]
impl RaftNetworkFactory<GlareTypeConfig> for Arc<ConsensusNetwork> {
    type Network = GlareNetworkConnection;
    type ConnectionError = NetworkError;

    async fn connect(
        &mut self,
        target: GlareNodeId,
        node: &GlareNode,
    ) -> Result<Self::Network, Self::ConnectionError> {
        dbg!(&node);
        let addr = &node.addr;
        let client = toy_rpc::Client::dial(addr).await.ok();
        Ok(GlareNetworkConnection {
            addr: addr.to_string(),
            client,
            target,
        })
    }
}

pub struct GlareNetworkConnection {
    addr: String,
    client: Option<toy_rpc::client::Client<AckModeNone>>,
    target: GlareNodeId,
}

type RpcClient = toy_rpc::client::Client<AckModeNone>;

impl GlareNetworkConnection {
    async fn client<E>(&mut self) -> RpcResult<&RpcClient, E>
    where
        E: std::error::Error,
    {
        if self.client.is_none() {
            self.client = toy_rpc::Client::dial(&self.addr).await.ok();
        }
        self.client
            .as_ref()
            .ok_or_else(|| RPCError::Network(NetworkError::from(AnyError::default())))
    }
}

#[async_trait]
impl RaftNetwork<GlareTypeConfig> for GlareNetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<GlareTypeConfig>,
    ) -> RpcResult<AppendEntriesResponse<GlareNodeId>, AppendEntriesError<GlareNodeId>> {
        self.client()
            .await?
            .raft()
            .append(req)
            .await
            .map_err(|e| to_error(e, self.target))
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<GlareTypeConfig>,
    ) -> RpcResult<InstallSnapshotResponse<GlareNodeId>, InstallSnapshotError<GlareNodeId>> {
        self.client()
            .await?
            .raft()
            .snapshot(req)
            .await
            .map_err(|e| to_error(e, self.target))
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<GlareNodeId>,
    ) -> RpcResult<VoteResponse<GlareNodeId>, VoteError<GlareNodeId>> {
        self.client()
            .await?
            .raft()
            .vote(req)
            .await
            .map_err(|e| to_error(e, self.target))
    }
}

fn to_error<E: std::error::Error + 'static + Clone>(
    e: toy_rpc::Error,
    target: GlareNodeId,
) -> RpcError<E> {
    match e {
        toy_rpc::Error::IoError(e) => RPCError::Network(NetworkError::new(&e)),
        toy_rpc::Error::ParseError(e) => RPCError::Network(NetworkError::new(&ErrWrap(e))),
        toy_rpc::Error::Internal(e) => {
            let any: &dyn Any = &e;
            let error: &E = any.downcast_ref().unwrap();
            RPCError::RemoteError(RemoteError::new(target, error.clone()))
        }
        e @ (toy_rpc::Error::InvalidArgument
        | toy_rpc::Error::ServiceNotFound
        | toy_rpc::Error::MethodNotFound
        | toy_rpc::Error::ExecutionError(_)
        | toy_rpc::Error::Canceled(_)
        | toy_rpc::Error::Timeout(_)
        | toy_rpc::Error::MaxRetriesReached(_)) => RPCError::Network(NetworkError::new(&e)),
    }
}

#[derive(Debug)]
struct ErrWrap(Box<dyn std::error::Error>);

impl std::fmt::Display for ErrWrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ErrWrap {}
