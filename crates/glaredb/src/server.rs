use anyhow::{anyhow, Result};
use futures::{SinkExt, TryStreamExt};
use lemur::execute::stream::source::DataSource;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sqlengine::engine::Engine;
use sqlengine::engine::{ExecutionResult, Session};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_serde::formats::Bincode;
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
        // let mut stream = new_server_stream(socket);
        // while let Some(msg) = stream.try_next().await? {
        //     match msg {
        //         Request::Execute(query) => match sess.execute_query(&query).await {
        //             Ok(results) => stream.send(Response::ExecutionResults(results)).await?,
        //             Err(e) => {
        //                 warn!(%e, "failed to execute query");
        //                 stream.send(Response::Error(e.to_string())).await?;
        //             }
        //         },
        //     }
        // }
        Ok(())
    }
}
