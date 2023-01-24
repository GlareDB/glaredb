use std::net::{SocketAddr, ToSocketAddrs};

use openssh::{ForwardType, KnownHosts, Session, SessionBuilder};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use std::io;
use tokio::net::TcpListener;
use tracing::{debug, trace};

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
        target_host: &str,
        target_port: u16,
    ) -> Result<(Session, SocketAddr)> {
        let host = "192.168.1.175".to_owned();
        let port = 22;
        let tunnel = SessionBuilder::default()
            .known_hosts_check(KnownHosts::Accept)
            .user_known_hosts_file("/dev/null")
            // .user(self.user.clone())
            // .port(self.port)
            .user("rustom".to_owned())
            .port(port)
            .keyfile("~/.ssh/glaredb") //FIXME use temp file path
            // .connect(self.host.clone())
            .connect(host.clone())
            .await?;

        // delete temp private key/
        tunnel.check().await?;
        trace!("rustom-create-tunnel check ok");

        // Find open local port and attempt to create tunnel
        for _ in 0..10 {
            let local_addr = generate_random_port().await?;

            trace!("local port attempt {}", local_addr.port());

            let target_addr = (target_host, target_port);

            let local = openssh::Socket::new(&local_addr)?;
            let remote = openssh::Socket::new(&target_addr)?;

            let lp = if let openssh::Socket::TcpSocket(addr) = local {
                addr.port()
            } else {
                todo!();
            };
            trace!("local port used {lp}");

            match tunnel
                .request_port_forward(ForwardType::Local, local, remote)
                .await
            {
                Ok(_) => return Ok((tunnel, local_addr)),
                Err(err) => match err {
                    openssh::Error::Ssh(err)
                        if err.to_string().contains("forwarding request failed") =>
                    {
                        trace!("rustom-create-tunnel");
                        debug!("port already in use, testing another port");
                    }
                    e => {
                        trace!("rustom-create-tunnel");
                        return Err(internal!("Cannot establish SSH tunnel: {e}"));
                    }
                },
            };
        }

        Err(internal!(
            "failed to find an open port to open the SSH tunnel"
        ))
    }
}

/// Generate random port using operating system by using port 0.
async fn generate_random_port() -> Result<SocketAddr, io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to bind to a random port due to {e}"),
        )
    })?;
    listener.local_addr()
}
