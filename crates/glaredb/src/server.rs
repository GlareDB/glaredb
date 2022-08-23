use anyhow::{anyhow, Result};
use futures::{SinkExt, TryStreamExt};
use lemur::execute::stream::source::DataSource;
use parking_lot::Mutex;
use pgsrv::handler::Handler;
use serde::{Deserialize, Serialize};
use sqlengine::engine::Engine;
use sqlengine::engine::{ExecutionResult, Session};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_serde::formats::Bincode;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, info, warn};

pub struct ServerConfig {
    pub pg_listener: TcpListener,
}

pub struct Server<S> {
    pg_handler: Arc<Handler<S>>,
}

impl<S: DataSource + 'static> Server<S> {
    pub fn new(source: S) -> Self {
        Server {
            pg_handler: Arc::new(Handler::new(Engine::new(source))),
        }
    }

    pub async fn serve(self, conf: ServerConfig) -> Result<()> {
        loop {
            let (conn, client_addr) = conf.pg_listener.accept().await?;
            let pg_handler = self.pg_handler.clone();
            tokio::spawn(async move {
                debug!(%client_addr, "client connected (pg)");
                match pg_handler.handle_connection(conn).await {
                    Ok(_) => debug!(%client_addr, "client disconnected"),
                    Err(e) => debug!(%e, %client_addr, "client disconnected with error."),
                }
            });
        }
    }
}
