use super::keys::{Key, KeySet};
use super::protocol::{Message, StateMachine};
use super::topology::{Address, Topology};
use super::{
    AccordError, Executor, NodeId, ReadResponse, Request, Response, Result, WriteResponse,
};
use futures::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use log::{debug, error, info};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::marker::Unpin;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_serde::formats::SymmetricalBincode;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const DEFAULT_BUFFER: usize = 512;

pub struct Client<K> {
    requests: mpsc::Sender<(Request<K>, oneshot::Sender<Response>)>,
}

impl<K: Key> Client<K> {
    pub async fn read(&self, keys: KeySet<K>, command: Vec<u8>) -> Result<ReadResponse> {
        match self.request(Request::Read { keys, command }).await? {
            Response::Read(resp) => Ok(resp),
            resp => Err(AccordError::ServerError(format!(
                "unexpected response for read: {:?}",
                resp
            ))),
        }
    }

    pub async fn write(&self, keys: KeySet<K>, command: Vec<u8>) -> Result<WriteResponse> {
        match self.request(Request::Write { keys, command }).await? {
            Response::Write(resp) => Ok(resp),
            resp => Err(AccordError::ServerError(format!(
                "unexpected response write: {:?}",
                resp
            ))),
        }
    }

    async fn request(&self, req: Request<K>) -> Result<Response> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.requests
            .send((req, resp_tx))
            .await
            .map_err(|e| AccordError::ServerError(format!("failed to send request: {:?}", e)))?;
        resp_rx
            .await
            .map_err(|e| AccordError::ServerError(format!("failed to receive response: {:?}", e)))
    }
}

#[derive(Debug)]
pub struct Server<K> {
    requests: mpsc::Receiver<(Request<K>, oneshot::Sender<Response>)>,
    peers: HashMap<NodeId, String>,
    // state: StateMachine<K, E>,
    local: NodeId,
}

impl<K: Key> Server<K> {
    /// Create a new server and client for that server.
    pub fn new(
        local: NodeId,
        // state: StateMachine<K, E>,
        peers: HashMap<NodeId, String>,
    ) -> (Client<K>, Server<K>) {
        let (reqs_tx, reqs_rx) = mpsc::channel(DEFAULT_BUFFER);
        (
            Client { requests: reqs_tx },
            Server {
                requests: reqs_rx,
                peers,
                // state,
                local,
            },
        )
    }

    pub async fn serve(self, listener: TcpListener) -> Result<()> {
        let (msg_in_tx, msg_in_rx) = mpsc::channel::<Message<K>>(DEFAULT_BUFFER);
        let (msg_out_tx, msg_out_rx) = mpsc::channel::<Message<K>>(DEFAULT_BUFFER);

        let listen_handle = tokio::spawn(listen(listener, msg_in_tx));
        let outbound_handle = tokio::spawn(connect_all(self.local, self.peers, msg_out_rx));

        unimplemented!()
    }

    // async fn handle_messages(requests: mpsc::Rece)
}

async fn listen<K>(listener: TcpListener, messages: mpsc::Sender<Message<K>>) -> Result<()>
where
    K: Key,
{
    loop {
        let (socket, _) = listener
            .accept()
            .await
            .map_err(|e| AccordError::ServerError(format!("failed to accept: {}", e)))?;

        let messages = messages.clone();
        tokio::spawn(async move {
            match listen_received_messages(socket, messages).await {
                Ok(()) => info!("peer disconnected"),
                Err(e) => error!("peer error: {}", e),
            }
        });
    }
}

async fn listen_received_messages<K>(
    socket: TcpStream,
    messages: mpsc::Sender<Message<K>>,
) -> Result<()>
where
    K: Key,
{
    let mut stream = SymmetricallyFramed::new(
        Framed::new(socket, LengthDelimitedCodec::new()),
        SymmetricalBincode::<Message<K>>::default(),
    );
    // TODO: Handle error
    while let Some(msg) = stream.try_next().await.unwrap() {
        messages
            .send(msg)
            .await
            .map_err(|e| AccordError::ServerError(format!("message send: {:?}", e)))?;
    }
    Ok(())
}

async fn connect_all<K>(
    local: NodeId,
    peers: HashMap<NodeId, String>,
    mut messages: mpsc::Receiver<Message<K>>,
) -> Result<()>
where
    K: Key,
{
    let mut peer_txs = HashMap::with_capacity(peers.len());
    for (node, addr) in peers.into_iter() {
        let (tx, rx) = mpsc::channel::<Message<K>>(DEFAULT_BUFFER);
        peer_txs.insert(node, tx);
        tokio::spawn(connect(addr, rx));
    }

    while let Some(msg) = messages.recv().await {
        let nodes = match msg.to {
            Address::Local => vec![local.clone()],
            Address::Peer(peer) => vec![peer],
            Address::Peers => peer_txs.keys().cloned().collect(),
        };

        for node in nodes.into_iter() {
            let msg = msg.clone();
            match peer_txs.get_mut(&node) {
                Some(tx) => match tx.send(msg).await {
                    Ok(()) => (),
                    Err(e) => error!("failed to send message: {}", e),
                },
                None => error!("missing channel for node: {}", node),
            }
        }
    }

    Ok(())
}

async fn connect<K>(addr: String, mut messages: mpsc::Receiver<Message<K>>)
where
    K: Key,
{
    loop {
        match TcpStream::connect(&addr).await {
            Ok(socket) => {
                info!("connected to peer: {}", addr);
                match stream_sent_messages(socket, &mut messages).await {
                    Ok(()) => {
                        info!("messages channel closes");
                        return;
                    }
                    Err(e) => error!("failed to send to peer: {}", e), // Continue on
                }
            }
            Err(err) => error!("failed to connect to peer: {}, {}", addr, err),
        }
    }
}

async fn stream_sent_messages<K>(
    socket: TcpStream,
    messages: &mut mpsc::Receiver<Message<K>>,
) -> Result<()>
where
    K: Key,
{
    let mut stream = SymmetricallyFramed::new(
        Framed::new(socket, LengthDelimitedCodec::new()),
        SymmetricalBincode::<Message<K>>::default(),
    );
    while let Some(msg) = messages.recv().await {
        stream
            .send(msg)
            .await
            .map_err(|e| AccordError::ServerError(format!("tcp stream send: {:?}", e)))?;
    }
    Ok(())
}
