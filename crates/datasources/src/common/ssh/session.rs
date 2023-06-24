use std::fmt::Display;
use std::fs::Permissions;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::unix::prelude::PermissionsExt;
use std::str::FromStr;
use std::time::Duration;

use ssh_key::{LineEnding, PrivateKey};
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::fs::File;
use tokio::net::TcpListener;
use tracing::{debug, trace};

use crate::common::errors::{DatasourceCommonError, Result};
use crate::common::ssh::SshKey;

#[derive(Debug)]
pub struct SshTunnelSession {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    inner: unix_impl::SshTunnelSessionImpl,
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    inner: not_unix_impl::SshTunnelSessionImpl,
}

#[derive(Debug, Clone)]
pub struct SshTunnelAccess {
    pub connection_string: String,
    pub keypair: SshKey,
}

impl SshTunnelAccess {
    /// Create an ssh tunnel using port fowarding from a random local port to
    /// the host specified in `connection_string`.
    pub async fn create_tunnel<T>(&self, remote_addr: &T) -> Result<(SshTunnelSession, SocketAddr)>
    where
        T: ToSocketAddrs,
    {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let (inner, addr) =
            unix_impl::create_tunnel(remote_addr, &self.connection_string, &self.keypair).await?;
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let (inner, addr) =
            not_unix_impl::create_tunnel(remote_addr, &self.connection_string, &self.keypair)
                .await?;
        Ok((SshTunnelSession { inner }, addr))
    }
}

/// Unix implementation for the ssh tunnel. This implementation is tested and
/// what's exposed in Cloud.
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod unix_impl {
    use super::*;
    use openssh::{ForwardType, KnownHosts, Session, SessionBuilder};

    #[derive(Debug)]
    pub struct SshTunnelSessionImpl(pub(super) Session);

    pub(super) async fn create_tunnel<T>(
        remote_addr: &T,
        connection_str: &str,
        keypair: &SshKey,
    ) -> Result<(SshTunnelSessionImpl, SocketAddr)>
    where
        T: ToSocketAddrs,
    {
        let temp_keyfile = generate_temp_keyfile(keypair.to_openssh()?.as_ref()).await?;

        let tunnel = SessionBuilder::default()
            .known_hosts_check(KnownHosts::Accept)
            .keyfile(temp_keyfile.path())
            // Set control directory explicitly. Otherwise we run the the
            // chance of an error like the following:
            //
            // 'path ... too long for Unix domain socket'
            .control_directory(std::env::temp_dir())
            // Wait 15 seconds before timing out ssh connection attempt
            .connect_timeout(Duration::from_secs(15))
            .connect(connection_str)
            .await?;

        // Check the status of the connection before proceeding.
        tunnel.check().await?;

        // Find open local port and attempt to create tunnel
        // Retry generating a port up to 10 times
        for _ in 0..10 {
            let local_addr = generate_random_port().await?;

            let local = openssh::Socket::new(&local_addr)?;
            let remote = openssh::Socket::new(remote_addr)?;

            match tunnel
                .request_port_forward(ForwardType::Local, local, remote)
                .await
            {
                // Tunnel successfully created
                Ok(()) => return Ok((SshTunnelSessionImpl(tunnel), local_addr)),
                Err(err) => match err {
                    openssh::Error::Ssh(err)
                        if err.to_string().contains("forwarding request failed") =>
                    {
                        debug!("port already in use, testing another port");
                    }
                    e => {
                        return Err(DatasourceCommonError::SshPortForward(e));
                    }
                },
            };
        }
        // If unable to find a port after 10 attempts
        Err(DatasourceCommonError::NoOpenPorts)
    }

    /// Generate temproary keyfile using the given private_key
    async fn generate_temp_keyfile(private_key: &str) -> Result<NamedTempFile> {
        let temp_keyfile = tempfile::Builder::new()
            .prefix("ssh_tunnel_key-")
            .tempfile()?;
        trace!(temp_keyfile = ?temp_keyfile.path(), "Temporary keyfile location");

        let keyfile = File::open(&temp_keyfile.path()).await?;
        // Set keyfile to only owner read and write permissions
        keyfile
            .set_permissions(Permissions::from_mode(0o600))
            .await?;

        fs::write(temp_keyfile.path(), private_key.as_bytes()).await?;

        // Remove write permission from file to prevent clobbering
        keyfile
            .set_permissions(Permissions::from_mode(0o400))
            .await?;

        Ok(temp_keyfile)
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
}

/// A stub implementation that returns always returns unsupported errors when
/// trying to create a tunnel.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod not_unix_impl {
    use super::*;

    #[derive(Debug)]
    pub struct SshTunnelSessionImpl;

    pub(super) async fn create_tunnel<T>(
        remote_addr: &T,
        connection_str: &str,
        keypair: &SshKey,
    ) -> Result<(SshTunnelSessionImpl, SocketAddr)>
    where
        T: ToSocketAddrs,
    {
        Err(DatasourceCommonError::Unsupported("SSH tunnels on windows"))
    }
}
