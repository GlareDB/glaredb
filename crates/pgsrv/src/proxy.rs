use crate::errors::Result;
use futures::Future;
use std::future::IntoFuture;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::log::debug;

use crate::errors::PgSrvError;

pub struct ConnectionProxy {
    client_stream: TcpStream,
    db_stream: TcpStream,
}

impl ConnectionProxy {
    pub fn new(client_stream: TcpStream, db_stream: TcpStream) -> Self {
        debug!("new proxy");

        Self {
            client_stream,
            db_stream,
        }
    }
}

impl IntoFuture for ConnectionProxy {
    type Output = Result<(), PgSrvError>;
    type IntoFuture = impl Future<Output = Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        transfer(self.client_stream, self.db_stream)
    }
}

/// Given an inbound connection, connect to the server and proxy the data.
async fn transfer(mut inbound: TcpStream, mut outbound: TcpStream) -> Result<()> {
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
