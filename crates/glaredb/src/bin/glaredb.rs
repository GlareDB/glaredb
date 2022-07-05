use anyhow::{anyhow, Result};
use clap::Parser;
use diststore::engine::{local::LocalEngine, StorageEngine};
use diststore::store::Store;
use futures::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use glaredb::message::{new_server_stream, Request, Response, ResponseInner, SerializableBatch};
use log::{error, info};
use sqlengine::engine::{Engine, ExecutionResult, Session};
use tokio::net::{TcpListener, TcpStream};

#[derive(Parser)]
#[clap(name = "GlareDB")]
#[clap(about = "SQL engine server component for GlareDB")]
struct Cli {
    #[clap(short, long, value_parser, default_value_t = String::from("localhost:6570"))]
    listen: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    logutil::init();
    let cli = Cli::parse();

    let server = Server::new_local()?;
    server.listen(&cli.listen).await
}

pub struct Server<S> {
    engine: Engine<S>,
}

impl Server<LocalEngine> {
    /// Create a server using a local storage engine.
    fn new_local() -> Result<Self> {
        info!("using local storage engine");
        let store = Store::new();
        let storage = LocalEngine::new(store);
        let mut engine = Engine::new(storage);
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
