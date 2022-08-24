use anyhow::Result;
use lemur::execute::stream::source::DataSource;
use pgsrv::handler::Handler;
use sqlengine::engine::Engine;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::debug;

pub struct ServerConfig {
    pub pg_listener: TcpListener,
}

pub struct Server<S> {
    pg_handler: Arc<Handler<S>>,
}

impl<S: DataSource + 'static> Server<S> {
    /// Connect to the given source, performing any bootstrap steps as
    /// necessary.
    pub async fn connect(source: S) -> Result<Self> {
        let mut engine = Engine::new(source);
        engine.ensure_system_tables().await?;
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
