use std::net::SocketAddr;

use openssh::{ForwardType, KnownHosts, Session, SessionBuilder};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use std::io;
use tokio::net::TcpListener;
use tracing::debug;

use crate::errors::{internal, Result};

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Serialize, Deserialize)]
pub struct SshKey {
    pub private_key: String,
    pub public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshTunnelAccess {
    pub host: String,
    pub user: String,
    pub port: u16,
    pub key: Option<SshKey>,
}

impl SshTunnelAccess {
    //FIXME: return port used in tunnel
    pub async fn create_tunnel(
        &self,
        remote_host: &str,
        remote_port: u16,
    ) -> Result<(Session, SocketAddr)> {
        let tunnel = SessionBuilder::default()
            .known_hosts_check(KnownHosts::Accept)
            .user(self.user.clone())
            .port(self.port)
            .keyfile("~/.ssh/glaredb") //FIXME use temp file path
            .connect(self.host.as_str())
            .await?;

        // delete temp private key/
        tunnel.check().await?;

        // Find open local port and attempt to create tunnel
        // Retry generating a port up to 10 times
        for _ in 0..10 {
            let local_addr = generate_random_port().await?;
            let remote_addr = (remote_host, remote_port);

            let local = openssh::Socket::new(&local_addr)?;
            let remote = openssh::Socket::new(&remote_addr)?;

            match tunnel
                .request_port_forward(ForwardType::Local, local, remote)
                .await
            {
                // Tunnel successfully created
                Ok(()) => return Ok((tunnel, local_addr)),
                Err(err) => match err {
                    openssh::Error::Ssh(err)
                        if err.to_string().contains("forwarding request failed") =>
                    {
                        debug!("port already in use, testing another port");
                    }
                    e => {
                        return Err(internal!("Cannot establish SSH tunnel: {e}"));
                    }
                },
            };
        }
        // If unable to find a port after 10 attempts
        Err(internal!(
            "failed to find an open port to open the SSH tunnel"
        ))
    }
}

/// Generate random port using operating system by using port 0.
async fn generate_random_port() -> Result<SocketAddr, io::Error> {
    // The 0 port indicates to the OS to assign a random port
    let listener = TcpListener::bind("localhost:0").await.map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to bind to a random port due to {e}"),
        )
    })?;
    listener.local_addr()
}
