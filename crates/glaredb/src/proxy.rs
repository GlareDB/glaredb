use anyhow::{anyhow, Result};
use pgsrv::auth::CloudAuthenticator;
use pgsrv::proxy::ProxyHandler;
use pgsrv::ssl::SslConfig;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::debug;

pub struct ProxyConfig {}

pub struct Proxy {
    handler: Arc<ProxyHandler<CloudAuthenticator>>,
}

impl Proxy {
    pub async fn new(
        api_addr: String,
        ssl_server_cert: Option<String>,
        ssl_server_key: Option<String>,
    ) -> Result<Self> {
        let ssl_conf = match (ssl_server_cert, ssl_server_key) {
            (Some(cert), Some(key)) => Some(SslConfig::new(cert, key)?),
            (None, None) => None,
            _ => {
                return Err(anyhow!(
                    "both or neither of the server key and cert must be provided"
                ))
            }
        };

        let auth = CloudAuthenticator::new(api_addr)?;
        Ok(Proxy {
            handler: Arc::new(ProxyHandler::new(auth, ssl_conf)),
        })
    }

    /// Start proxying connections from the given listener to the server.
    pub async fn serve(self, listener: TcpListener) -> Result<()> {
        while let Ok((inbound, _)) = listener.accept().await {
            let handler = self.handler.clone();
            tokio::spawn(async move {
                debug!("client connected (proxy)");
                match handler.proxy_connection(inbound).await {
                    Ok(_) => debug!("client disconnected"),
                    Err(e) => debug!(%e, "client disconnected with error."),
                }
            });
        }

        Ok(())
    }
}
