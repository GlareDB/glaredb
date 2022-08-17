use async_trait::async_trait;
use futures::{Sink, SinkExt, Stream, StreamExt, TryStream};
use openraft::{
    error::{AppendEntriesError, InstallSnapshotError, RPCError, VoteError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    BasicNode, RaftNetwork, RaftNetworkFactory,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};
use tokio_serde::formats::SymmetricalBincode;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, trace};

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("channel broken")]
    ChannelBroken,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

use crate::raft::{NodeId, TypeConfig};

/// Wrapper type around messages sent and received via `RaftNetwork`.
#[derive(Debug, Serialize, Deserialize)]
enum Message {
    AppendEntriesRequest(AppendEntriesRequest<TypeConfig>),
    InstallSnapshotRequest(InstallSnapshotRequest<TypeConfig>),
    VoteRequest(VoteRequest<NodeId>),
}

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for Client {
    type Network = ClusterClient;
    type ConnectionError = NetworkError;

    async fn connect(
        &mut self,
        target: NodeId,
        node: &BasicNode,
    ) -> Result<Self::Network, Self::ConnectionError> {
        // TODO: Provide node id as well here.
        self.try_connect(node.addr.clone()).await
    }
}

#[async_trait]
impl RaftNetwork<TypeConfig> for ClusterClient {
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

#[derive(Debug, Clone)]
pub struct ClusterClient {
    sender: mpsc::Sender<Message>,
}

impl ClusterClient {}

/// A request to connect to another node in the cluster.
#[derive(Debug)]
pub struct ConnRequest {
    addr: String,
}

/// A response to a connection request.
#[derive(Debug)]
pub struct ConnResponse {
    result: Result<ClusterClient, NetworkError>,
}

/// The primary interface to local server.
pub struct Client {
    requests: mpsc::UnboundedSender<(ConnRequest, oneshot::Sender<ConnResponse>)>,
}

impl Client {
    /// Try to get a client to another node in the cluster.
    pub async fn try_connect(&self, addr: String) -> Result<ClusterClient, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.requests
            .send((ConnRequest { addr }, tx))
            .map_err(|_| NetworkError::ChannelBroken)?;
        let resp = rx.await.map_err(|_| NetworkError::ChannelBroken)?;
        resp.result
    }
}

const DEBUG_LOG_TICK: Duration = Duration::from_secs(10);
const PEER_CHANNEL_SIZE: usize = 256;

pub struct Server {
    requests: mpsc::UnboundedReceiver<(ConnRequest, oneshot::Sender<ConnResponse>)>,
    /// Peers that we have open connections to.
    peers: HashMap<String, mpsc::Sender<Message>>,
}

impl Server {
    pub fn new() -> (Server, Client) {
        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        (
            Server {
                requests: requests_rx,
                peers: HashMap::new(),
            },
            Client {
                requests: requests_tx,
            },
        )
    }

    /// Start serving with the given listener.
    pub async fn serve(mut self, listener: TcpListener) -> Result<(), NetworkError> {
        let mut debug_ticker = time::interval(DEBUG_LOG_TICK);
        loop {
            select! {
                _ = debug_ticker.tick() => debug!("debug tick"),

                Some((req, sender)) = self.requests.recv() => {
                    let resp = self.connect_to_peer(req.addr).await;
                    sender.send(ConnResponse{ result: resp });
                }
            }
        }
    }

    /// Connect to a peer with the given address.
    ///
    /// If there's already an open connection, it will be reused. Otherwise a
    /// new connection will be opened.
    async fn connect_to_peer(&mut self, addr: String) -> Result<ClusterClient, NetworkError> {
        // Already connection, nothing to do.
        if let Some(sender) = self.peers.get(&addr) {
            return Ok(ClusterClient {
                sender: sender.clone(),
            });
        }

        let (sender, mut receiver) = mpsc::channel(PEER_CHANNEL_SIZE);
        let conn_addr = addr.clone();
        tokio::spawn(async move {
            loop {
                match TcpStream::connect(&conn_addr).await {
                    Ok(socket) => {
                        debug!(%conn_addr, "connected to peer");
                        match Self::send_peer(socket, &mut receiver).await {
                            Ok(_) => return,
                            Err(e) => error!(%e, %conn_addr, "failed to send to peer"),
                        }
                    }
                    Err(e) => error!(%e, %conn_addr, "failed to connect to peer"),
                }
                time::sleep(Duration::from_secs(1)).await;
            }
        });

        // Note that even though we got here, there's no guarantee that we've
        // successfully connected to a peer node.
        //
        // TODO: Do we want to try to detect errors connecting here?

        self.peers.insert(addr, sender.clone());
        Ok(ClusterClient { sender })
    }

    /// Continually send messages from `receiver` into the provided tcp stream
    /// until exhaustion.
    async fn send_peer(
        socket: TcpStream,
        receiver: &mut mpsc::Receiver<Message>,
    ) -> Result<(), NetworkError> {
        let mut stream = new_message_stream(socket);
        while let Some(msg) = receiver.recv().await {
            stream.send(msg).await?;
        }
        Ok(())
    }
}

type MessageStream = tokio_serde::SymmetricallyFramed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Message,
    SymmetricalBincode<Message>,
>;

fn new_message_stream(socket: TcpStream) -> MessageStream {
    tokio_serde::SymmetricallyFramed::new(
        Framed::new(socket, LengthDelimitedCodec::new()),
        SymmetricalBincode::default(),
    )
}
