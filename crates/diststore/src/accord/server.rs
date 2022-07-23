use super::keys::{Key, KeySet};
use super::log::Log;
use super::node::Node;
use super::protocol::{Message, ProtocolMessage};
use super::topology::{Address, TopologyManagerRef};
use super::{Executor, NodeId, ReadResponse, Request, Response, WriteResponse};
use anyhow::{anyhow, Context, Result};
use futures::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use tracing::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::marker::Unpin;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};
use tokio_serde::formats::SymmetricalBincode;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const DEFAULT_BUFFER: usize = 512;

#[derive(Debug, Clone)]
pub struct Client<K> {
    requests: mpsc::Sender<(Request<K>, oneshot::Sender<Response>)>,
}

impl<K: Key> Client<K> {
    pub fn new(requests: mpsc::Sender<(Request<K>, oneshot::Sender<Response>)>) -> Self {
        Client { requests }
    }

    pub async fn read(&self, keys: KeySet<K>, command: Vec<u8>) -> Result<ReadResponse> {
        match self.request(Request::Read { keys, command }).await? {
            Response::Read(resp) => Ok(resp),
            resp => Err(anyhow!("unexpected response for read: {:?}", resp)),
        }
    }

    pub async fn write(&self, keys: KeySet<K>, command: Vec<u8>) -> Result<WriteResponse> {
        match self.request(Request::Write { keys, command }).await? {
            Response::Write(resp) => Ok(resp),
            resp => Err(anyhow!("unexpected response write: {:?}", resp)),
        }
    }

    async fn request(&self, req: Request<K>) -> Result<Response> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.requests
            .send((req, resp_tx))
            .await
            .map_err(|e| anyhow!("failed to send request: {:?}", e))?;
        resp_rx
            .await
            .map_err(|e| anyhow!("failed to receive response: {:?}", e))
    }
}

#[derive(Debug)]
pub struct Server<K> {
    local: NodeId,
    tm: TopologyManagerRef,
    requests: mpsc::Receiver<(Request<K>, oneshot::Sender<Response>)>,
    inbound: mpsc::UnboundedSender<Message<K>>,
    outbound: mpsc::UnboundedReceiver<Message<K>>,

    req_id_gen: AtomicU32,
    pending: HashMap<u32, oneshot::Sender<Response>>,
}

impl<K: Key> Server<K> {
    /// Create a new server.
    ///
    /// `requests` corresponds the the client channel.
    ///
    /// `inbound` and `outbound` correspond to channels that get passed into a
    /// `Node` on start.
    pub fn new(
        local: NodeId,
        tm: TopologyManagerRef,
        requests: mpsc::Receiver<(Request<K>, oneshot::Sender<Response>)>,
        inbound: mpsc::UnboundedSender<Message<K>>,
        outbound: mpsc::UnboundedReceiver<Message<K>>,
    ) -> Result<Self> {
        Ok(Server {
            local,
            tm,
            requests,
            inbound,
            outbound,
            req_id_gen: AtomicU32::new(0),
            pending: HashMap::new(),
        })
    }

    pub async fn serve(self, listener: TcpListener) -> Result<()> {
        let addr = listener.local_addr()?;
        info!("serving accord requests on {}", addr);

        let (incoming_tx, incoming_rx) = mpsc::channel::<Message<K>>(DEFAULT_BUFFER);
        let listen_handle = tokio::spawn(tcp_listen(listener, incoming_tx));

        let (outgoing_tx, outgoing_rx) = mpsc::channel::<Message<K>>(DEFAULT_BUFFER);
        let send_handle = tokio::spawn(stream_to_all_peers(
            self.local,
            self.tm.clone(),
            outgoing_rx,
        ));

        let messages_handle = tokio::spawn(self.handle_messages(incoming_rx, outgoing_tx));

        // TODO: Handle errors other than the join error.
        let _ = tokio::try_join!(listen_handle, send_handle, messages_handle)?;

        Ok(())
    }

    async fn handle_messages(
        mut self,
        mut incoming_rx: mpsc::Receiver<Message<K>>,
        outgoing_tx: mpsc::Sender<Message<K>>,
    ) -> Result<()> {
        loop {
            select! {
                // Messages from clients.
                Some((client_req, resp_tx)) = self.requests.recv() => {
                    let id = self.req_id_gen.fetch_add(1, Ordering::Relaxed);
                    debug!("assigned id {} to client message with keys: {:?}", id, &client_req.keys());
                    self.pending.insert(id, resp_tx);
                    let proto = match client_req {
                        Request::Read{keys, command} => ProtocolMessage::BeginRead{keys,command},
                        Request::Write{keys, command} => ProtocolMessage::BeginWrite{keys,command},
                    };
                    let msg = Message {from: self.local, to: Address::Local, req: id, proto_msg: proto};
                    outgoing_tx.send(msg).await.map_err(|_| anyhow!("failed send client message"))?;
                }

                // Messages from other nodes.
                Some(msg) = incoming_rx.recv() => {
                    match msg {
                        Message {req, proto_msg: ProtocolMessage::ReadResponse{data}, ..} => {
                            debug!("sending read response to client for request {}", req);
                            match self.pending.remove(&req) {
                                Some(tx) => if let Err(_) = tx.send(Response::Read(ReadResponse{data: data.data})) {
                                    warn!("failed to send read response for req {}", req);
                                }
                                None => error!("missing response channel for req {}", req),
                            }
                        }
                        Message {req, proto_msg: ProtocolMessage::WriteResponse{data}, ..} => {
                            debug!("sending write response to client for request {}", req);
                            match self.pending.remove(&req) {
                                Some(tx) => if let Err(_) = tx.send(Response::Write(WriteResponse{data: data.data})) {
                                    warn!("failed to send write response for req {}", req);
                                }
                                None => error!("missing response channel for req {}", req),
                            }
                        }
                        msg => {
                            self.inbound.send(msg).map_err(|_| anyhow!("failed send inbound"))?;
                        },
                    }
                },

                // Messages to be sent to other nodes.
                Some(msg) = self.outbound.recv() => {
                    outgoing_tx.send(msg).await.map_err(|_| anyhow!("failed send outgoing"))?;
                }
            }
        }
    }
}

/// Continually accept connections from the listener.
///
/// Every message from any connection will be sent to the provided channel.
async fn tcp_listen<K: Key>(
    listener: TcpListener,
    incoming: mpsc::Sender<Message<K>>,
) -> Result<()> {
    loop {
        let (socket, addr) = listener.accept().await?;
        let incoming = incoming.clone();
        tokio::spawn(async move {
            debug!("peer connected: {}", addr);
            let mut stream = new_message_stream(socket);
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(msg) => {
                        if let Err(e) = incoming.send(msg).await {
                            error!("failed to send incoming message: {}", e);
                        }
                    }
                    Err(e) => error!("failed to get next message: {}", e),
                }
            }
        });
    }
}

/// Stream outgoing messages to peers.
async fn stream_to_all_peers<K: Key>(
    local: NodeId,
    tm: TopologyManagerRef,
    mut outgoing: mpsc::Receiver<Message<K>>,
) -> Result<()> {
    let mut peers = HashMap::new();
    for (id, addr) in tm.get_current().iter_peers() {
        let (tx, rx) = mpsc::channel(DEFAULT_BUFFER);
        peers.insert(*id, tx);
        tokio::spawn(stream_to_peer(addr.clone(), rx));
    }

    while let Some(msg) = outgoing.recv().await {
        let to = match &msg.to {
            Address::Local => vec![local],
            Address::Peer(id) => vec![*id],
            Address::Peers => peers.keys().cloned().collect(),
        };

        for id in to.into_iter() {
            match peers.get_mut(&id) {
                Some(tx) => match tx.try_send(msg.clone()) {
                    Ok(_) => (),
                    Err(e) => error!("failed to send for peer {}: {}", id, e),
                },
                None => error!("unknown peer: {}", id),
            }
        }
    }
    Ok(())
}

/// Open a tcp connection using the provided address, and the stream the
/// messages from `outgoing` to that peer.
///
/// Failures to connect will result in retries.
///
/// Returns when `outgoing` is closed.
async fn stream_to_peer<K: Key>(addr: SocketAddr, mut outgoing: mpsc::Receiver<Message<K>>) {
    loop {
        match TcpStream::connect(addr).await {
            Ok(socket) => {
                debug!("connected to peer {}", addr);
                let mut stream = new_message_stream(socket);
                while let Some(msg) = outgoing.recv().await {
                    match stream.send(msg).await {
                        Ok(_) => (),
                        Err(e) => error!("failed to send to peer: {}", e),
                    }
                }
                // Channel closed.
                return;
            }
            Err(e) => info!("failed to connect to peer: {}, retrying...", e),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accord::keys::ExactString;
    use crate::accord::protocol::{Message, ProtocolMessage};
    use crate::accord::topology::{Address, Topology, TopologyManager};
    use std::sync::Arc;

    fn new_server(
        local: NodeId,
        tm: TopologyManagerRef,
    ) -> (
        Server<ExactString>,
        mpsc::Sender<(Request<ExactString>, oneshot::Sender<Response>)>,
        mpsc::UnboundedReceiver<Message<ExactString>>,
        mpsc::UnboundedSender<Message<ExactString>>,
    ) {
        let (c_tx, c_rx) = mpsc::channel(16);
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();

        let server = Server::new(local, tm, c_rx, inbound_tx, outbound_rx).unwrap();
        (server, c_tx, inbound_rx, outbound_tx)
    }

    fn assert_in_channel_empty<T>(ch: &mut mpsc::UnboundedReceiver<T>) {
        match ch.try_recv() {
            Ok(_) => panic!("channel not empty"),
            Err(mpsc::error::TryRecvError::Empty) => (),
            Err(_) => panic!("channel disconnected"),
        }
    }

    /// Create and start 3 servers, returning the communication channels.
    ///
    /// All vectors have three items corresponding to each of the three servers.
    /// The index corresponds to the node id.
    async fn create_servers() -> (
        Vec<mpsc::Sender<(Request<ExactString>, oneshot::Sender<Response>)>>,
        Vec<mpsc::UnboundedReceiver<Message<ExactString>>>,
        Vec<mpsc::UnboundedSender<Message<ExactString>>>,
    ) {
        // Create listeners.
        let listeners = vec![
            TcpListener::bind("0.0.0.0:0").await.unwrap(),
            TcpListener::bind("0.0.0.0:0").await.unwrap(),
            TcpListener::bind("0.0.0.0:0").await.unwrap(),
        ];

        let nodes = vec![0, 1, 2];

        // Create topology using the addresses from the listener.
        let members: Vec<_> = nodes
            .into_iter()
            .zip(
                listeners
                    .iter()
                    .map(|listener| listener.local_addr().unwrap()),
            )
            .collect();
        let topology = Topology::new(members.clone(), members).unwrap();
        let tm = Arc::new(TopologyManager::new(topology));

        // Create and start servers.
        let (s1, c1, in1, out1) = new_server(0, tm.clone());
        let (s2, c2, in2, out2) = new_server(1, tm.clone());
        let (s3, c3, in3, out3) = new_server(2, tm.clone());
        let servers = [s1, s2, s3].into_iter().zip(listeners.into_iter());

        for (server, listener) in servers {
            tokio::spawn(async move { server.serve(listener).await.unwrap() });
        }

        (
            vec![c1, c2, c3],
            vec![in1, in2, in3],
            vec![out1, out2, out3],
        )
    }

    #[tokio::test]
    async fn server_send_receive() {
        logutil::init_test();

        let (_, mut ins, outs) = create_servers().await;

        // Send to self.
        let msg = Message {
            from: 0,
            to: Address::Local,
            req: 0,
            proto_msg: ProtocolMessage::Ping,
        };
        outs[0].send(msg.clone()).unwrap();
        let got = ins[0].recv().await.unwrap();
        assert_eq!(msg, got);
        assert_in_channel_empty(&mut ins[1]);
        assert_in_channel_empty(&mut ins[2]);

        // Send to peer.
        let msg = Message {
            from: 0,
            to: Address::Peer(1),
            req: 0,
            proto_msg: ProtocolMessage::Ping,
        };
        outs[0].send(msg.clone()).unwrap();
        let got = ins[1].recv().await.unwrap();
        assert_eq!(msg, got);
        assert_in_channel_empty(&mut ins[0]);
        assert_in_channel_empty(&mut ins[2]);

        // Send to all.
        let msg = Message {
            from: 1,
            to: Address::Peers,
            req: 0,
            proto_msg: ProtocolMessage::Ping,
        };
        outs[1].send(msg.clone()).unwrap();
        for in_ch in ins.iter_mut() {
            let got = in_ch.recv().await.unwrap();
            assert_eq!(msg, got);
            // Make sure we have no leftover messages.
            assert_in_channel_empty(in_ch);
        }
    }

    #[tokio::test]
    async fn client_messages_forwarded() {
        logutil::init_test();

        let (clients, mut ins, _) = create_servers().await;

        let req = Request::Read {
            keys: KeySet::from_key(ExactString("hello".to_string())),
            command: vec![],
        };

        let (tx, _rx) = oneshot::channel();
        clients[0].send((req, tx)).await.unwrap();

        // TODO: Check that we get the message we expect.
        let _ = ins[0].recv().await.unwrap();
    }
}
