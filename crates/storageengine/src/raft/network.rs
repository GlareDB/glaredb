use async_trait::async_trait;
use futures::{Sink, SinkExt, Stream, StreamExt, TryStream, TryStreamExt};
use openraft::{
    error::{AppendEntriesError, InstallSnapshotError, RPCError, VoteError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    BasicNode, RaftNetwork, RaftNetworkFactory,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};
use tokio_serde::formats::SymmetricalBincode;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, info, trace};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("channel broken")]
    ChannelBroken,
    #[error("unexpected client response: {0:?}")]
    UnexpectedClientResponse(Box<ClientResponse>),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

use crate::raft::{NodeId, TypeConfig};

#[derive(Debug, Serialize, Deserialize)]
pub struct RaftEnvelope {
    /// ID of the request this message pertains to.
    pub id: Uuid,
    /// ID of node this message is for.
    pub to: NodeId,
    pub msg: RaftMessage,
}

/// A conglomeration of all raft messages.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftMessage {
    Request(RaftRequest),
    Response(RaftResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRequest {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RaftResponse {
    AppendEntries(AppendEntriesResponse<NodeId>),
    InstallSnapshot(InstallSnapshotResponse<NodeId>),
    Vote(VoteResponse<NodeId>),
}

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for Client {
    type Network = TargetedRaftClient;
    type ConnectionError = NetworkError;

    async fn connect(
        &mut self,
        target: NodeId,
        node: &BasicNode,
    ) -> Result<Self::Network, Self::ConnectionError> {
        self.try_connect(target, node.addr.clone()).await?;
        Ok(TargetedRaftClient {
            node: target,
            client: self.clone(),
        })
    }
}

#[async_trait]
impl RaftNetwork<TypeConfig> for TargetedRaftClient {
    async fn send_append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, AppendEntriesError<NodeId>>,
    > {
        // TODO: Handle errors.
        match self
            .send_raft_req(RaftRequest::AppendEntries(rpc))
            .await
            .unwrap()
        {
            RaftResponse::AppendEntries(res) => Ok(res),
            other => panic!("unexpected raft response: {:?}", other),
        }
    }

    async fn send_install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, InstallSnapshotError<NodeId>>,
    > {
        // TODO: Handle errors.
        match self
            .send_raft_req(RaftRequest::InstallSnapshot(rpc))
            .await
            .unwrap()
        {
            RaftResponse::InstallSnapshot(res) => Ok(res),
            other => panic!("unexpected raft response: {:?}", other),
        }
    }

    async fn send_vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, VoteError<NodeId>>> {
        // TODO: Handle errors.
        match self.send_raft_req(RaftRequest::Vote(rpc)).await.unwrap() {
            RaftResponse::Vote(res) => Ok(res),
            other => panic!("unexpected raft response: {:?}", other),
        }
    }
}

/// A wrapper around a `Client` that sends only raft requests.
#[derive(Debug)]
pub struct TargetedRaftClient {
    node: NodeId,
    client: Client,
}

impl TargetedRaftClient {
    fn from_client(client: Client, node: NodeId) -> TargetedRaftClient {
        TargetedRaftClient { node, client }
    }

    async fn send_raft_req(&self, req: RaftRequest) -> Result<RaftResponse, NetworkError> {
        match self
            .client
            .request(ClientRequest::Raft { id: self.node, req })
            .await?
        {
            ClientResponse::Raft { resp } => Ok(resp),
            other => Err(NetworkError::UnexpectedClientResponse(Box::new(other))),
        }
    }
}

#[derive(Debug)]
pub enum ClientRequest {
    /// Connect to a node in the cluster with the given addr.
    Connect {
        id: NodeId,
        addr: String,
    },
    /// Register a local raft node to receive messages.
    Register {
        id: NodeId,
        messages: mpsc::UnboundedSender<RaftRequest>,
    },
    Raft {
        id: NodeId,
        req: RaftRequest,
    },
}

/// A response to a client request.
#[derive(Debug)]
pub enum ClientResponse {
    /// The result of trying to connect to a node in the cluster.
    Connect,
    /// The result of trying to register a new node.
    Register,
    Raft {
        resp: RaftResponse,
    },
}

/// The primary interface to local server.
#[derive(Debug, Clone)]
pub struct Client {
    requests: mpsc::UnboundedSender<(ClientRequest, oneshot::Sender<ClientResponse>)>,
}

impl Client {
    async fn try_connect(&self, id: NodeId, addr: String) -> Result<(), NetworkError> {
        match self.request(ClientRequest::Connect { id, addr }).await {
            Ok(ClientResponse::Connect) => Ok(()),
            Ok(other) => Err(NetworkError::UnexpectedClientResponse(Box::new(other))),
            Err(e) => Err(e),
        }
    }

    async fn register(
        &self,
        id: NodeId,
        messages: mpsc::UnboundedSender<RaftRequest>,
    ) -> Result<(), NetworkError> {
        match self.request(ClientRequest::Register { id, messages }).await {
            Ok(ClientResponse::Register) => Ok(()),
            Ok(other) => Err(NetworkError::UnexpectedClientResponse(Box::new(other))),
            Err(e) => Err(e),
        }
    }

    async fn request(&self, req: ClientRequest) -> Result<ClientResponse, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.requests
            .send((req, tx))
            .map_err(|_| NetworkError::ChannelBroken)?;
        let resp = rx.await.map_err(|_| NetworkError::ChannelBroken)?;
        Ok(resp)
    }
}

const DEBUG_LOG_TICK: Duration = Duration::from_secs(10);
const PEER_CHANNEL_SIZE: usize = 256;

#[derive(Debug)]
struct LocalNodeChannel {
    messages: mpsc::UnboundedSender<RaftRequest>,
}

#[derive(Debug)]
struct RemoteNodeChannel {
    addr: String,
    messages: mpsc::Sender<RaftEnvelope>,
}

pub struct Server {
    requests: mpsc::UnboundedReceiver<(ClientRequest, oneshot::Sender<ClientResponse>)>,
    /// Local nodes.
    local: HashMap<NodeId, LocalNodeChannel>,
    /// Remote peers that we have open connections to.
    remote: HashMap<NodeId, RemoteNodeChannel>,
    pending: HashMap<Uuid, oneshot::Sender<ClientResponse>>,
}

impl Server {
    pub fn new() -> (Server, Client) {
        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        (
            Server {
                requests: requests_rx,
                local: HashMap::new(),
                remote: HashMap::new(),
                pending: HashMap::new(),
            },
            Client {
                requests: requests_tx,
            },
        )
    }

    /// Start serving with the given listener.
    pub async fn serve(mut self, listener: TcpListener) -> Result<(), NetworkError> {
        // Any sends to this channel should immediately return from the function
        // with the result sent on the channel.
        let (join_sender, mut join_receiver) = mpsc::unbounded_channel();
        let (incoming_sender, mut incoming_receiver) = mpsc::unbounded_channel();

        // Start up the listener.
        tokio::spawn(async move {
            let result = Self::listen(listener, incoming_sender).await;
            let _ = join_sender.send(result);
        });

        let mut debug_ticker = time::interval(DEBUG_LOG_TICK);
        loop {
            select! {
                // Send on the join channel, exit with the result.
                Some(join_result) = join_receiver.recv() => return join_result,

                _ = debug_ticker.tick() => debug!("debug tick"),

                Some(msg) = incoming_receiver.recv() => self.handle_peer_msg(msg).await,

                Some((req, sender)) = self.requests.recv() => self.handle_client_req(req, sender).await,
            }
        }
    }

    /// Accept messages from peers on the provided listener, sending all
    /// messages from all peers that connect to this server to the provided
    /// channel.
    async fn listen(
        listener: TcpListener,
        incoming_sender: mpsc::UnboundedSender<RaftEnvelope>,
    ) -> Result<(), NetworkError> {
        loop {
            let (socket, addr) = listener.accept().await?;
            debug!(%addr, "peer connected");
            let sender = incoming_sender.clone();
            tokio::spawn(async move {
                match Self::recv_peer(socket, sender.clone()).await {
                    Ok(_) => debug!(%addr, "peer disconnected"),
                    Err(e) => error!(%e, %addr, "error receiving from peer"),
                }
            });
        }
    }

    async fn handle_client_req(
        &mut self,
        req: ClientRequest,
        response: oneshot::Sender<ClientResponse>,
    ) {
        let req_id = Uuid::new_v4();
        self.pending.insert(req_id.clone(), response);

        match req {
            ClientRequest::Connect { id, addr } => {
                self.connect_to_peer(id, addr).await.unwrap(); // TODO: Handle error.
                self.send_response(&req_id, ClientResponse::Connect);
            }
            ClientRequest::Register { id, messages } => {
                self.local.insert(id, LocalNodeChannel { messages });
                self.send_response(&req_id, ClientResponse::Register);
            }
            ClientRequest::Raft { id, req } => {
                let node = self.remote.get(&id).unwrap(); // TODO: Handle missing node.
                let envelope = RaftEnvelope {
                    id: req_id,
                    to: id,
                    msg: RaftMessage::Request(req),
                };
                node.messages.send(envelope).await.unwrap(); // TODO: Handle error.
            }
            _ => unimplemented!(),
        }
    }

    /// Send out a (local) client response with the given id, and remove it from
    /// the pending map.
    fn send_response(&mut self, id: &Uuid, resp: ClientResponse) {
        if let Some(sender) = self.pending.remove(id) {
            sender.send(resp); // TODO: Handle error (channel close).
        }
    }

    async fn handle_peer_msg(&mut self, msg: RaftEnvelope) {
        match msg.msg {
            RaftMessage::Request(req) => {
                let node = self.local.get(&msg.to).unwrap(); // TODO: Handle.
                node.messages.send(req).unwrap(); // TODO: Handle.
            }
            RaftMessage::Response(resp) => {
                self.send_response(&msg.id, ClientResponse::Raft { resp })
            }
        }
    }

    /// Connect to a peer with the given address.
    ///
    /// If there's already an open connection, it will be reused. Otherwise a
    /// new connection will be opened.
    async fn connect_to_peer(&mut self, id: NodeId, addr: String) -> Result<(), NetworkError> {
        // Already connection, nothing to do.
        if let Some(client) = self.remote.get(&id) {
            return Ok(());
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
        self.remote.insert(
            id,
            RemoteNodeChannel {
                addr,
                messages: sender,
            },
        );
        Ok(())
    }

    /// Continually read messages from the tcp stream, sending them into the
    /// provided channel.
    async fn recv_peer(
        socket: TcpStream,
        sender: mpsc::UnboundedSender<RaftEnvelope>,
    ) -> Result<(), NetworkError> {
        let mut stream = new_message_stream(socket);
        while let Some(msg) = stream.try_next().await? {
            sender.send(msg).map_err(|e| NetworkError::ChannelBroken)?;
        }
        Ok(())
    }

    /// Continually send messages from `receiver` into the provided tcp stream
    /// until exhaustion.
    async fn send_peer(
        socket: TcpStream,
        receiver: &mut mpsc::Receiver<RaftEnvelope>,
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
    RaftEnvelope,
    SymmetricalBincode<RaftEnvelope>,
>;

fn new_message_stream(socket: TcpStream) -> MessageStream {
    tokio_serde::SymmetricallyFramed::new(
        Framed::new(socket, LengthDelimitedCodec::new()),
        SymmetricalBincode::default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::Vote;

    fn dummy_vote_request() -> VoteRequest<NodeId> {
        VoteRequest {
            vote: Vote {
                term: 0,
                node_id: 0,
                committed: false,
            },
            last_log_id: None,
        }
    }

    fn dummy_vote_response() -> VoteResponse<NodeId> {
        VoteResponse {
            vote: Vote {
                term: 0,
                node_id: 0,
                committed: false,
            },
            vote_granted: true,
            last_log_id: None,
        }
    }

    #[tokio::test]
    async fn simple() {
        logutil::init_test();

        let (s1, c1) = Server::new();
        let l1 = TcpListener::bind("localhost:0").await.unwrap();
        let addr1 = l1.local_addr().unwrap().to_string();
        tokio::spawn(s1.serve(l1));

        let (s2, c2) = Server::new();
        let l2 = TcpListener::bind("localhost:0").await.unwrap();
        let addr2 = l2.local_addr().unwrap().to_string();
        tokio::spawn(s2.serve(l2));

        let (tx1, rx1) = mpsc::unbounded_channel();
        c1.register(1, tx1).await.unwrap();

        let (tx2, mut rx2) = mpsc::unbounded_channel();
        c2.register(2, tx2).await.unwrap();

        c1.try_connect(2, addr2).await.unwrap();
        let mut raft1 = TargetedRaftClient::from_client(c1, 2);
        c2.try_connect(1, addr1).await.unwrap();
        let mut raft2 = TargetedRaftClient::from_client(c2, 1);

        // TODO: The rest
    }
}
