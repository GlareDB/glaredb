use std::fs::Permissions;
use std::io;
use std::net::SocketAddr;
use std::os::unix::prelude::PermissionsExt;

use openssh::{ForwardType, KnownHosts, Session, SessionBuilder};
use ssh_key::sec1::der::zeroize::Zeroizing;
use ssh_key::{LineEnding, PrivateKey};
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::fs::File;
use tokio::net::TcpListener;
use tracing::{debug, trace};

use crate::errors::{internal, Result};

#[derive(Debug, Clone)]
pub struct SshKey {
    keypair: PrivateKey,
}

impl SshKey {
    /// Generate a random Ed25519 ssh key pair
    pub fn generate_random() -> Result<Self> {
        let keypair = PrivateKey::random(rand::thread_rng(), ssh_key::Algorithm::Ed25519)?;
        Ok(Self { keypair })
    }

    /// Recreate ssh key from bytes store in catalog
    pub fn from_bytes(keypair: &[u8]) -> Result<Self> {
        let keypair = PrivateKey::from_bytes(keypair)?;
        Ok(Self { keypair })
    }

    /// Serialize sshk key as raw bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.keypair.to_bytes()?.to_vec())
    }

    /// Create an OpenSSH-formatted public key as a String
    pub fn public_key(&self) -> Result<String> {
        Ok(self.keypair.public_key().to_openssh()?)
    }

    /// Create an OpenSSH-formatted private key as a String
    pub(crate) fn private_key(&self) -> Result<Zeroizing<String>> {
        Ok(self.keypair.to_openssh(LineEnding::default())?)
    }
}

#[derive(Debug, Clone)]
pub struct SshTunnelAccess {
    pub host: String,
    pub user: String,
    pub port: u16,
    pub keypair: SshKey,
}

impl SshTunnelAccess {
    pub async fn create_tunnel(
        &self,
        remote_host: &str,
        remote_port: u16,
    ) -> Result<(Session, SocketAddr)> {
        let temp_keyfile =
            Self::generate_temp_keyfile(self.keypair.private_key()?.as_ref()).await?;

        let tunnel = SessionBuilder::default()
            .known_hosts_check(KnownHosts::Accept)
            .user(self.user.clone())
            .port(self.port)
            .keyfile(temp_keyfile.path())
            .connect(self.host.as_str())
            .await?;

        tunnel.check().await?;

        // Find open local port and attempt to create tunnel
        // Retry generating a port up to 10 times
        for _ in 0..10 {
            let local_addr = Self::generate_random_port().await?;
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

    /// Generate temproary keyfile using the given private_key
    async fn generate_temp_keyfile(private_key: &str) -> Result<NamedTempFile> {
        let temp_keyfile = tempfile::Builder::new()
            .prefix("ssh_tunnel_key-")
            .tempfile()?;
        trace!("Temporary keyfile location {:?}", temp_keyfile.path());

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

#[cfg(test)]
mod tests {
    use crate::ssh::{SshKey, SshTunnelAccess};

    #[tokio::test]
    async fn validate_temp_keyfile() {
        let key = SshKey::generate_random().unwrap();

        let private_key = key.private_key().unwrap();

        let temp_keyfile = SshTunnelAccess::generate_temp_keyfile(private_key.as_ref())
            .await
            .unwrap();

        let keyfile_data = std::fs::read(temp_keyfile.path()).unwrap();

        assert_eq!(keyfile_data, private_key.as_bytes());
    }
}
