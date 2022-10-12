use std::future::IntoFuture;
use std::sync::Arc;
use anyhow::Result;
use pgsrv::proxy::ConnectionProxy;
use pgsrv::handler::{PostgresHandler, ProxyHandler};
use tokio::net::{TcpListener, TcpStream};
use tracing::debug;

pub struct ProxyConfig {}

pub struct Proxy {
    server_addr: String,
    handler: Arc<ProxyHandler>,
}

impl Proxy {
    pub async fn new(server_addr: &str) -> Result<Self> {
        Ok(Proxy {
            server_addr: server_addr.to_string(),
            handler: Arc::new(ProxyHandler::new()),
        })
    }

    /// Start proxying connections from the given listener to the server.
    pub async fn serve(self, listener: TcpListener) -> Result<()> {
        while let Ok((inbound, _)) = listener.accept().await {
            let handler = self.handler.clone();
            tokio::spawn(async move {
                debug!("client connected (proxy)");
                match handler.handle_connection(inbound).await {
                    Ok(_) => debug!("client disconnected"),
                    Err(e) => debug!(%e, "client disconnected with error."),
                }
            });

        }

        Ok(())
    }
}
