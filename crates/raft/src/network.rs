use std::str::FromStr;
use std::sync::Arc;

use openraft::async_trait::async_trait;
use openraft::error::NetworkError;
use openraft::{RaftNetwork, RaftNetworkFactory};
use tonic::transport::Endpoint;

use crate::openraft_types::types::{
    AppendEntriesError, AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotError,
    InstallSnapshotRequest, InstallSnapshotResponse, VoteError, VoteRequest, VoteResponse,
};

use super::error::{RpcError, RpcResult};
use super::repr::{Node, NodeId, RaftTypeConfig};

pub struct ConsensusNetwork {}

#[async_trait]
impl RaftNetworkFactory<RaftTypeConfig> for Arc<ConsensusNetwork> {
    type Network = GlareNetworkConnection;
    type ConnectionError = NetworkError;

    async fn connect(
        &mut self,
        target: NodeId,
        node: &Node,
    ) -> Result<Self::Network, Self::ConnectionError> {
        dbg!(&node);
        let endpoint = Endpoint::from_str(&node.address)
            .map_err(|e| NetworkError::new(&e))?;

        match RpcClient::connect(endpoint).await {
            Ok(client) => Ok(GlareNetworkConnection {
                client,
                target,
            }),
            Err(e) => Err(NetworkError::new(&e)),
        }
    }
}

pub struct GlareNetworkConnection {
    client: RpcClient,
    target: NodeId,
}

type RpcClient = crate::rpc::pb::raft_network_client::RaftNetworkClient<tonic::transport::Channel>;

#[async_trait]
impl RaftNetwork<RaftTypeConfig> for GlareNetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest,
    ) -> RpcResult<AppendEntriesResponse, AppendEntriesError> {
        let req: crate::rpc::pb::AppendEntriesRequest = req.try_into().expect("invalid request");
        self.client
            .append_entries(req)
            .await
            .map_err(|e| tonic_rpc_error(e, self.target))
            .map(|r| r.into_inner().try_into().expect("invalid response"))
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest,
    ) -> RpcResult<InstallSnapshotResponse, InstallSnapshotError> {
        let req: crate::rpc::pb::InstallSnapshotRequest = req.try_into().expect("invalid request");
        self.client
            .snapshot(req)
            .await
            .map_err(|e| tonic_rpc_error(e, self.target))
            .map(|r| r.into_inner().try_into().expect("invalid response"))
    }

    async fn send_vote(&mut self, req: VoteRequest) -> RpcResult<VoteResponse, VoteError> {
        let req: crate::rpc::pb::VoteRequest = req.try_into().expect("vote request");

        self.client
            .vote(req)
            .await
            .map_err(|e| tonic_rpc_error(e, self.target))
            .map(|r| r.into_inner().try_into().expect("vote response"))
    }
}

fn tonic_rpc_error<E: std::error::Error + 'static + Clone>(
    e: tonic::Status,
    _target: NodeId,
) -> RpcError<E> {
    /*
    match e {
        toy_rpc::Error::IoError(e) => RpcError::Network(NetworkError::new(&e)),
        toy_rpc::Error::ParseError(e) => RpcError::Network(NetworkError::new(&ErrWrap(e))),
        toy_rpc::Error::Internal(e) => {
            let any: &dyn Any = &e;
            let error: &E = any.downcast_ref().unwrap();
            RpcError::RemoteError(RemoteError::new(target, error.clone()))
        }
        e @ (toy_rpc::Error::InvalidArgument
        | toy_rpc::Error::ServiceNotFound
        | toy_rpc::Error::MethodNotFound
        | toy_rpc::Error::ExecutionError(_)
        | toy_rpc::Error::Canceled(_)
        | toy_rpc::Error::Timeout(_)
        | toy_rpc::Error::MaxRetriesReached(_)) => RpcError::Network(NetworkError::new(&e)),
    }
     */

    RpcError::Network(NetworkError::new(&e))
}


#[derive(Debug)]
struct ErrWrap(Box<dyn std::error::Error>);

impl std::fmt::Display for ErrWrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ErrWrap {}
