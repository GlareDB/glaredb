use crate::engine::Engine;
use crate::engine::{ExecutionResult, Session};
use anyhow::{anyhow, Result};
use futures::{SinkExt, TryStreamExt};
use lemur::execute::stream::source::DataSource;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_serde::formats::{Bincode};

use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, info, warn};

#[derive(Debug)]
pub struct Server<S> {
    engine: Arc<Engine<S>>,
}

impl<S: DataSource + 'static> Server<S> {
    pub fn new(engine: Engine<S>) -> Self {
        Server {
            engine: Arc::new(engine),
        }
    }

    /// Start listening on the provided addr and serve sql requests.
    pub async fn serve(self, addr: &str) -> Result<()> {
        // TODO: Add listener for accord/storage.
        let listener = TcpListener::bind(addr).await?;
        info!("listening on {}", addr);

        loop {
            let (socket, client_addr) = listener.accept().await?;
            let engine = self.engine.clone();
            tokio::spawn(async move {
                debug!("client connected, addr: {}", client_addr);
                match engine.begin_session() {
                    Ok(session) => match Self::handle_conn(socket, session).await {
                        Ok(_) => debug!(%client_addr, "session finished"),
                        Err(e) => error!(%e, "session exited with error"),
                    },
                    Err(e) => error!(%e, %client_addr, "failed to begin session for connection"),
                }
            });
        }
    }

    async fn handle_conn(socket: TcpStream, mut sess: Session<S>) -> Result<()> {
        let mut stream = new_server_stream(socket);
        while let Some(msg) = stream.try_next().await? {
            match msg {
                Request::Execute(query) => match sess.execute_query(&query).await {
                    Ok(results) => stream.send(Response::ExecutionResults(results)).await?,
                    Err(e) => {
                        warn!(%e, "failed to execute query");
                        stream.send(Response::Error(e.to_string())).await?;
                    }
                },
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Client {
    /// Connection to the server. Currently behind a mutex to ensure only one
    /// request happens at a time. Eventually that will change such that each
    /// request is tagged with some identifer to allow for concurrent streaming.
    stream: Mutex<ClientStream>,
}

impl Client {
    /// Connect to a server at address.
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Client> {
        let socket = TcpStream::connect(addr).await?;
        let stream = Mutex::new(new_client_stream(socket));
        Ok(Client { stream })
    }

    /// Execute a query.
    pub async fn execute(&self, query: String) -> Result<Vec<ExecutionResult>> {
        // Currently assumes that for every request, we only get a single
        // response back. This assumption may change in the future.
        let mut stream = self.stream.lock();
        stream.send(Request::Execute(query)).await?;
        Ok(match stream.try_next().await? {
            Some(Response::ExecutionResults(results)) => results,
            Some(other) => return Err(anyhow!("unexpected response: {:?}", other)),
            None => return Err(anyhow!("disconnected")),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    /// Execute one or many queries.
    Execute(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    ExecutionResults(Vec<ExecutionResult>),
    Error(String),
}

type ClientStream = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Response,
    Request,
    Bincode<Response, Request>,
>;

pub fn new_client_stream(socket: TcpStream) -> ClientStream {
    tokio_serde::Framed::new(
        Framed::new(socket, LengthDelimitedCodec::new()),
        Bincode::default(),
    )
}

type ServerStream = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Request,
    Response,
    Bincode<Request, Response>,
>;

pub fn new_server_stream(socket: TcpStream) -> ServerStream {
    tokio_serde::Framed::new(
        Framed::new(socket, LengthDelimitedCodec::new()),
        Bincode::default(),
    )
}
