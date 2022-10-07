use anyhow::Result;
use futures::FutureExt;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::debug;

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
            let transfer = transfer(inbound, self.server_addr.clone()).map(|r| {
                if let Err(e) = r {
                    debug!("error: {}", e);
                }
            });

            tokio::spawn(transfer);
        }

        Ok(())
    }
}

/// Given an inbound connection, connect to the server and proxy the data.
async fn transfer(mut inbound: TcpStream, proxy_addr: String) -> Result<()> {
    let mut outbound = TcpStream::connect(proxy_addr).await?;

    let (mut inbound_reader, mut inbound_writer) = inbound.split();
    let (mut outbound_reader, mut outbound_writer) = outbound.split();

    let client_to_server = async {
        tokio::io::copy(&mut inbound_reader, &mut outbound_writer).await?;
        outbound_writer.shutdown().await
    };
    let server_to_client = async {
        tokio::io::copy(&mut outbound_reader, &mut inbound_writer).await?;
        inbound_writer.shutdown().await
    };

    tokio::try_join!(client_to_server, server_to_client)?;

    Ok(())
}
