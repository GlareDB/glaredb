use std::future::IntoFuture;

use anyhow::Result;
use pgsrv::proxy::ConnectionProxy;
use tokio::net::{TcpListener, TcpStream};

pub struct ProxyConfig {}

pub struct Proxy {
    server_addr: String,
}

impl Proxy {
    pub async fn new(server_addr: &str) -> Result<Self> {
        Ok(Proxy {
            server_addr: server_addr.to_string(),
        })
    }

    /// Start proxying connections from the given listener to the server.
    pub async fn serve(self, listener: TcpListener) -> Result<()> {
        while let Ok((inbound, _)) = listener.accept().await {
            let proxy = TcpStream::connect(self.server_addr.clone())
                .await
                .map(|db_stream| ConnectionProxy::new(inbound, db_stream))?;

            tokio::spawn(proxy.into_future());
        }

        Ok(())
    }
}
