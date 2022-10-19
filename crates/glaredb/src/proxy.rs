use anyhow::Result;
use pgsrv::handler::{PostgresHandler, ProxyHandler};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::debug;

pub struct ProxyConfig {}

pub struct Proxy {
    handler: Arc<ProxyHandler>,
}

impl Proxy {
    pub async fn new(api_addr: String) -> Result<Self> {
        Ok(Proxy {
            handler: Arc::new(ProxyHandler::new(api_addr)),
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
