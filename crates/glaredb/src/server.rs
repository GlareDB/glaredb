use anyhow::Result;
use object_store_util::temp::TempObjectStore;
use pgsrv::handler::Handler;
use sqlexec::engine::Engine;
use sqlexec::runtime::AccessRuntime;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, info};

pub struct ServerConfig {
    pub pg_listener: TcpListener,
}

pub struct Server {
    pg_handler: Arc<Handler>,
}

impl Server {
    /// Connect to the given source, performing any bootstrap steps as
    /// necessary.
    pub async fn connect(db_name: impl Into<String>) -> Result<Self> {
        // TODO: Provide the access runtime to the server.
        let store = Arc::new(TempObjectStore::new()?);
        info!(%store, "object storage");
        let access = Arc::new(AccessRuntime::new(store));

        let engine = Engine::new(db_name, access)?;
        Ok(Server {
            pg_handler: Arc::new(Handler::new(engine)),
        })
    }

    /// Serve using the provided config.
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
