use super::keys::{Key, KeySet};
use super::log::Log;
use super::node::Node;
use super::protocol::Message;
use super::topology::{Address, TopologyManagerRef};
use super::{
    AccordError, Executor, NodeId, ReadResponse, Request, Response, Result, WriteResponse,
};
use futures::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::marker::Unpin;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio_serde::formats::SymmetricalBincode;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const DEFAULT_BUFFER: usize = 512;

#[derive(Debug, Clone)]
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
    local: NodeId,
    tm: TopologyManagerRef,
    requests: mpsc::Receiver<(Request<K>, oneshot::Sender<Response>)>,
    inbound: mpsc::UnboundedSender<Message<K>>,
    outbound: mpsc::UnboundedReceiver<Message<K>>,
}

impl<K: Key> Server<K> {
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
        })
    }

    pub async fn serve(self, bind: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(bind).await?;

        let (incoming_tx, incoming_rx) = mpsc::channel::<Message<K>>(DEFAULT_BUFFER);
        let listen_handle = tokio::spawn(tcp_listen(listener, incoming_tx));

        let (outgoing_tx, outgoing_rx) = mpsc::channel::<Message<K>>(DEFAULT_BUFFER);
        let send_handle = tokio::spawn(open_tcp_session_all(
            self.local,
            self.tm.clone(),
            outgoing_rx,
        ));

        let eventloop_handle = tokio::spawn(self.eventloop(incoming_rx, outgoing_tx));

        // TODO: Handle errors other than the join error.
        let _ = tokio::try_join!(listen_handle, send_handle, eventloop_handle)?;

        Ok(())
    }

    async fn eventloop(
        mut self,
        mut incoming_rx: mpsc::Receiver<Message<K>>,
        outgoing_tx: mpsc::Sender<Message<K>>,
    ) -> Result<()> {
        loop {
            select! {
                Some(msg) = incoming_rx.recv() => self.inbound.send(msg).map_err(|_| AccordError::ServerError("failed send inbound".to_string()))?,
                Some(msg) = self.outbound.recv() => outgoing_tx.send(msg).await.map_err(|_| AccordError::ServerError("failed send outgoing".to_string()))?,
            }
        }
    }
}

/// Continually accept connections from the listener, and send messages to the
/// provided channel.
async fn tcp_listen<K: Key>(
    listener: TcpListener,
    incoming: mpsc::Sender<Message<K>>,
) -> Result<()> {
    loop {
        let (socket, addr) = listener.accept().await?;
        let incoming = incoming.clone();
        tokio::spawn(async move {
            debug!("peer connected: {}", addr);
            let mut framed = SymmetricallyFramed::<_, Message<K>, _>::new(
                Framed::new(socket, LengthDelimitedCodec::new()),
                SymmetricalBincode::<Message<K>>::default(),
            );
            while let Some(msg) = framed.next().await {
                match msg {
                    Ok(msg) => {
                        if let Err(e) = incoming.send(msg).await {
                            error!("failed to send");
                        }
                    }
                    Err(e) => error!("failed to get next message: {}", e),
                };
            }
        });
    }
}

async fn open_tcp_session_all<K: Key>(
    local: NodeId,
    tm: TopologyManagerRef,
    mut outgoing: mpsc::Receiver<Message<K>>,
) -> Result<()> {
    let mut peers = HashMap::new();
    for (id, addr) in tm.get_current().iter_peers() {
        let (tx, rx) = mpsc::channel(DEFAULT_BUFFER);
        peers.insert(*id, tx);
        tokio::spawn(open_tcp_session(addr.clone(), rx));
    }

    while let Some(msg) = outgoing.recv().await {
        let to = match &msg.to {
            Address::Local => vec![local],
            Address::Peer(id) => vec![*id],
            Address::Peers => peers.keys().cloned().collect(),
        };

        for id in to.into_iter() {
            let msg = msg.clone();
            match peers.get_mut(&id) {
                Some(tx) => match tx.try_send(msg) {
                    Ok(_) => (),
                    Err(e) => error!("failed to send for peer {}: {}", id, e),
                },
                None => error!("unknown peer {}", id),
            }
        }
    }
    Ok(())
}

/// Continually open tcp connections to a node, sending messages from the
/// provided channel.
async fn open_tcp_session<K: Key>(addr: SocketAddr, mut outgoing: mpsc::Receiver<Message<K>>) {
    loop {
        match TcpStream::connect(addr).await {
            Ok(socket) => {
                debug!("connected to peer {}", addr);
                let mut framed = SymmetricallyFramed::<_, Message<K>, _>::new(
                    Framed::new(socket, LengthDelimitedCodec::new()),
                    SymmetricalBincode::<Message<K>>::default(),
                );
                while let Some(msg) = outgoing.recv().await {
                    match framed.send(msg).await {
                        Ok(_) => (),
                        Err(e) => error!("failed to send: {}", e),
                    }
                }
            }
            Err(e) => error!("failed to connect to peer: {}", e),
        }
    }
}
