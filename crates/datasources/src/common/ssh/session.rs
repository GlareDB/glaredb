use std::fs::Permissions;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use tempfile::NamedTempFile;
use tokio::fs;
use tokio::fs::File;
use tokio::net::TcpListener;
use tracing::{debug, trace};

use crate::common::ssh::key::{SshKey, SshKeyError};

#[derive(Debug, thiserror::Error)]
pub enum SshTunnelError {
    /// Generic openssh errors.
    ///
    /// Using debug to get the underlying errors (the openssh crate doesn't keep
    /// those in the message).
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[error("{0:?}")]
    OpenSsh(#[from] openssh::Error),

    /// Port forward error with openssh.
    ///
    /// Using debug to get the underlying errors (the openssh crate doesn't keep
    /// those in the message).
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[error("Cannot establish SSH tunnel: {0:?}")]
    SshPortForward(openssh::Error),

    #[error("No remote addresses provided")]
    NoRemoteAddressesProvided,

    #[error(transparent)]
    SshKey(#[from] SshKeyError),

    #[error("Failed to find an open port to open the SSH tunnel")]
    NoOpenPorts,

    #[error("SSH tunnels unsupported on this platform")]
    Unsupported,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Handle to the underlying session for the tunnel.
///
/// Dropping this will close the underlying connection if `close` has not
/// already been called.
#[derive(Debug)]
pub struct SshTunnelSession {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    inner: unix_impl::SshTunnelSessionImpl,
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    inner: not_unix_impl::SshTunnelSessionImpl,
}

impl SshTunnelSession {
    pub async fn close(self) -> Result<(), SshTunnelError> {
        self.inner.close().await
    }
}

/// Access configuration for opening the tunnel.
#[derive(Debug, Clone)]
pub struct SshTunnelAccess {
    pub connection_string: String,
    pub keypair: SshKey,
}

impl SshTunnelAccess {
    /// Create an ssh tunnel using port fowarding from a random local port to
    /// the host specified in `connection_string`.
    ///
    /// The returned session should be kept around for the desired lifetime of
    /// the tunnel. Once the session is dropped, the tunnel will be closed.
    ///
    /// Note that this will only work for macos and linux as it relies on
    /// openssh. Attempting to create a tunnel on windows will always return an
    /// error.
    pub async fn create_tunnel<T>(
        &self,
        remote_addr: &T,
    ) -> Result<(SshTunnelSession, SocketAddr), SshTunnelError>
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
    use super::{
        debug, fs, io, trace, Duration, File, NamedTempFile, Permissions, SocketAddr, SshKey,
        SshTunnelError, TcpListener, ToSocketAddrs,
    };
    use openssh::{ForwardType, KnownHosts, Session, SessionBuilder};
    use std::{
        net::{IpAddr, Ipv4Addr},
        os::unix::prelude::PermissionsExt,
    };

    #[derive(Debug)]
    pub struct SshTunnelSessionImpl(pub(super) Session);

    impl SshTunnelSessionImpl {
        pub(super) async fn close(self) -> Result<(), SshTunnelError> {
            self.0.close().await?;
            Ok(())
        }
    }

    pub(super) async fn create_tunnel<T>(
        remote_addr: &T,
        connection_str: &str,
        keypair: &SshKey,
    ) -> Result<(SshTunnelSessionImpl, SocketAddr), SshTunnelError>
    where
        T: ToSocketAddrs,
    {
        let temp_keyfile = generate_temp_keyfile(keypair.to_openssh()?.as_ref()).await?;

        let remote_addr = remote_addr
            .to_socket_addrs()?
            .next()
            .ok_or(SshTunnelError::NoRemoteAddressesProvided)?;

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
            let port = get_random_localhost_port().await?;
            let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);

            let local = openssh::Socket::from(local_addr);
            let remote = openssh::Socket::from(remote_addr);

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
                        return Err(SshTunnelError::SshPortForward(e));
                    }
                },
            };
        }
        // If unable to find a port after 10 attempts
        Err(SshTunnelError::NoOpenPorts)
    }

    /// Generate temproary keyfile using the given private_key
    async fn generate_temp_keyfile(private_key: &str) -> Result<NamedTempFile, SshTunnelError> {
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

    /// Get a random port to use for the localhost side of the ssh tunnel.
    ///
    /// Checking if the port is free is best-effort.
    async fn get_random_localhost_port() -> Result<u16, io::Error> {
        // The 0 port indicates to the OS to assign a random port
        let listener = TcpListener::bind("localhost:0").await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to bind to a random port due to {e}"),
            )
        })?;
        let addr = listener.local_addr()?;
        Ok(addr.port())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn validate_temp_keyfile() {
            let key = SshKey::generate_random().unwrap();
            let private_key = key.to_openssh().unwrap();
            let temp_keyfile = generate_temp_keyfile(private_key.as_ref()).await.unwrap();
            let keyfile_data = std::fs::read(temp_keyfile.path()).unwrap();
            assert_eq!(keyfile_data, private_key.as_bytes());
        }
    }
}

/// A stub implementation that returns always returns unsupported errors when
/// trying to create a tunnel.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod not_unix_impl {
    use super::*;

    #[derive(Debug)]
    pub struct SshTunnelSessionImpl;

    impl SshTunnelSessionImpl {
        pub(super) async fn close(self) -> Result<(), SshTunnelError> {
            Ok(())
        }
    }

    pub(super) async fn create_tunnel<T>(
        remote_addr: &T,
        connection_str: &str,
        keypair: &SshKey,
    ) -> Result<(SshTunnelSessionImpl, SocketAddr), SshTunnelError>
    where
        T: ToSocketAddrs,
    {
        Err(SshTunnelError::Unsupported)
    }
}
