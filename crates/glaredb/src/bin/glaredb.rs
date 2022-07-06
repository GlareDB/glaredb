use anyhow::{anyhow, Result};
use clap::Parser;
use diststore::accord::log::Log;
use diststore::accord::node::Node;
use diststore::accord::server::{Client, Server as AccordServer};
use diststore::accord::topology::{Topology, TopologyManager};
use diststore::accord::NodeId;
use diststore::engine::{
    accord::{AccordEngine, AccordExecutor},
    local::LocalEngine,
    StorageEngine,
};
use diststore::store::Store;
use futures::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use glaredb::message::{new_server_stream, Request, Response, ResponseInner, SerializableBatch};
use log::{error, info};
use sqlengine::engine::{Engine, ExecutionResult, Session};
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

#[derive(Parser)]
#[clap(name = "GlareDB")]
#[clap(about = "SQL engine server component for GlareDB")]
struct Cli {
    #[clap(short, long, value_parser, default_value_t = String::from("localhost:6570"))]
    listen: String,

    #[clap(long, value_parser)]
    local_id: Option<NodeId>,
    #[clap(long, value_parser)]
    local_addr: String,

    #[clap(long, value_parser)]
    peer_ids: Vec<NodeId>,
    #[clap(long, value_parser)]
    peer_addrs: Vec<String>,
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<()> {
    logutil::init();
    let cli = Cli::parse();

    // Assumes if "local-id" is provided, then when should use accord.
    match cli.local_id {
        Some(local) => {
            let mut peers: Vec<_> = cli
                .peer_ids
                .into_iter()
                .zip(cli.peer_addrs.into_iter())
                .collect();
            peers.push((local, cli.local_addr.to_string()));
            let server = Server::start_accord_engine(local, &cli.local_addr, &peers).await?;
            server.listen(&cli.listen).await
        }
        None => {
            let server = Server::start_local_engine()?;
            server.listen(&cli.listen).await
        }
    }
}

pub struct Server<S> {
    engine: Engine<S>,
}

impl Server<LocalEngine> {
    /// Create a server using a local storage engine.
    fn start_local_engine() -> Result<Server<LocalEngine>> {
        info!("using local storage engine");
        let store = Store::new();
        let storage = LocalEngine::new(store);
        let mut engine = Engine::new(storage);
        engine.ensure_system_tables()?;
        Ok(Server { engine })
    }
}

impl Server<AccordEngine> {
    async fn start_accord_engine(
        local_id: NodeId,
        local_addr: &str,
        peers: &[(NodeId, String)],
    ) -> Result<Server<AccordEngine>> {
        info!("using accord storage engine");
        let store = Store::new();
        let local = LocalEngine::new(store);
        let executor = AccordExecutor::new(local);

        let peers = peers
            .iter()
            .map(|(node, addr)| {
                let addr = addr.parse::<SocketAddr>()?;
                Ok((*node, addr))
            })
            .collect::<Result<Vec<_>>>()?;

        let topology = Topology::new(peers.clone(), peers)?;
        let tm = Arc::new(TopologyManager::new(topology));

        let (client_tx, client_rx) = mpsc::channel(256);
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();

        let _node = Node::start(
            Log::new(),
            local_id,
            tm.clone(),
            executor,
            inbound_rx,
            outbound_tx,
        )
        .await?;

        let server = AccordServer::new(local_id, tm, client_rx, inbound_tx, outbound_rx)?;
        let client = Client::new(client_tx);

        let listener = TcpListener::bind(local_addr).await?;
        tokio::spawn(server.serve(listener));

        let accord = AccordEngine::new(client);
        let mut engine = Engine::new(accord);

        engine.ensure_system_tables()?;

        Ok(Server { engine })
    }
}

impl<S: StorageEngine + 'static> Server<S> {
    async fn listen(self, listen_addr: &str) -> Result<()> {
        info!("attempting to listen on {}", listen_addr);

        let listener = TcpListener::bind(listen_addr).await?;
        loop {
            let (socket, remote) = listener.accept().await?;
            info!("received connection from {}", remote);
            match self.engine.start_session() {
                Ok(session) => match Self::handle_connection(session, socket).await {
                    Ok(_) => info!("client disconnected"),
                    Err(e) => error!("failed to handle client connection: {}", e),
                },
                Err(e) => error!("failed to create session for {}: {}", remote, e),
            };
        }
    }

    async fn handle_connection(mut session: Session<S>, socket: TcpStream) -> Result<()> {
        let mut stream = new_server_stream(socket);
        while let Some(msg) = stream.try_next().await? {
            match msg {
                Request::Execute(query) => {
                    let results = session.execute_query(&query).await?;
                    for result in results.into_iter() {
                        match result.result {
                            ExecutionResult::Other => {
                                stream
                                    .send(Response {
                                        duration: result.execution_duration,
                                        inner: ResponseInner::Other,
                                    })
                                    .await?
                            }
                            ExecutionResult::QueryResult { batches } => {
                                stream
                                    .send(Response {
                                        duration: result.execution_duration,
                                        inner: ResponseInner::QueryResult {
                                            batches: batches
                                                .into_iter()
                                                .map(SerializableBatch::from)
                                                .collect(),
                                        },
                                    })
                                    .await?
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
