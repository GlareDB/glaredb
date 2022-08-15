use async_trait::async_trait;
use openraft::{
    error::{AppendEntriesError, InstallSnapshotError, RPCError, VoteError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    BasicNode, RaftNetwork, RaftNetworkFactory,
};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};
use tokio_serde::formats::SymmetricalBincode;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {}

use crate::raft::{NodeId, TypeConfig};

/// Wrapper type around messages sent and received via `RaftNetwork`.
#[derive(Debug, Serialize, Deserialize)]
enum Message {
    AppendEntriesRequest(AppendEntriesRequest<TypeConfig>),
    InstallSnapshotRequest(InstallSnapshotRequest<TypeConfig>),
    VoteRequest(VoteRequest<NodeId>),
}

#[derive(Debug, Clone)]
pub struct Client {}

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for Client {
    type Network = AddressedClient;
    type ConnectionError = std::convert::Infallible;

    async fn connect(
        &mut self,
        target: NodeId,
        node: &BasicNode,
    ) -> Result<Self::Network, Self::ConnectionError> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct AddressedClient {
    addr: String,
}

#[async_trait]
impl RaftNetwork<TypeConfig> for AddressedClient {
    async fn send_append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, AppendEntriesError<NodeId>>,
    > {
        unimplemented!()
    }

    async fn send_install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, InstallSnapshotError<NodeId>>,
    > {
        unimplemented!()
    }

    async fn send_vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, VoteError<NodeId>>> {
        unimplemented!()
    }
}

pub struct Server {}

impl Server {}

type MessageStream<K> = tokio_serde::SymmetricallyFramed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Message<K>,
    SymmetricalBincode<Message<K>>,
>;

fn new_message_stream<K>(socket: TcpStream) -> MessageStream<K> {
    tokio_serde::SymmetricallyFramed::new(
        Framed::new(socket, LengthDelimitedCodec::new()),
        SymmetricalBincode::default(),
    )
}
