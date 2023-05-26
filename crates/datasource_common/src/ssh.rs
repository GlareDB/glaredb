use std::fmt::Display;
use std::fs::Permissions;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::unix::prelude::PermissionsExt;
use std::str::FromStr;
use std::time::Duration;

use openssh::{ForwardType, KnownHosts, Session, SessionBuilder};
use ssh_key::{LineEnding, PrivateKey};
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::fs::File;
use tokio::net::TcpListener;
use tracing::{debug, trace};

use crate::errors::{DatasourceCommonError, Result};

#[derive(Debug, Clone)]
pub struct SshKey {
    keypair: PrivateKey,
}

impl SshKey {
    /// Generate a random Ed25519 ssh key pair.
    pub fn generate_random() -> Result<Self> {
        let keypair = PrivateKey::random(rand::thread_rng(), ssh_key::Algorithm::Ed25519)?;
        Ok(Self { keypair })
    }

    /// Recreate ssh key from bytes store in catalog.
    pub fn from_bytes(keypair: &[u8]) -> Result<Self> {
        let keypair = PrivateKey::from_bytes(keypair)?;
        Ok(Self { keypair })
    }

    /// Serialize sshk key as raw bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.keypair.to_bytes()?.to_vec())
    }

    /// Create an OpenSSH-formatted public key as a String.
    pub fn public_key(&self) -> Result<String> {
        Ok(self.keypair.public_key().to_openssh()?)
    }

    /// Create an OpenSSH-formatted private key as a String.
    pub(crate) fn to_openssh(&self) -> Result<String> {
        Ok(self.keypair.to_openssh(LineEnding::default())?.to_string())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SshConnectionParameters {
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
}

impl FromStr for SshConnectionParameters {
    type Err = DatasourceCommonError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use DatasourceCommonError::SshConnectionParseError;

        let s = match s.strip_prefix("ssh://") {
            None => {
                return Err(SshConnectionParseError(format!(
                    "connection string should start with `ssh://`: {s}"
                )))
            }
            Some(s) => s,
        };

        let (user, address) = match s.split_once('@') {
            None => {
                return Err(SshConnectionParseError(format!(
                    "connection string should have the format `ssh://user@address`: {s}"
                )))
            }
            Some((user, address)) => (user, address),
        };

        if user.is_empty() {
            return Err(SshConnectionParseError(format!(
                "user cannot be empty: {s}"
            )));
        }

        if address.is_empty() {
            return Err(SshConnectionParseError(format!(
                "address cannot be empty: {s}"
            )));
        }

        // Find the `:` and split on it.
        let (host, port) = match address.rsplit_once(':') {
            None => (address.to_owned(), None),
            Some((host, port)) => {
                let port: u16 = port.parse().map_err(|_e| {
                    SshConnectionParseError(format!("port should be a valid number: {port}"))
                })?;
                (host.to_owned(), Some(port))
            }
        };

        if host.is_empty() {
            return Err(SshConnectionParseError(format!(
                "host cannot be empty: {s}"
            )));
        }

        Ok(Self {
            host,
            port,
            user: user.to_owned(),
        })
    }
}

#[derive(Debug)]
pub enum SshConnection {
    ConnectionString(String),
    Parameters(SshConnectionParameters),
}

impl Display for SshConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionString(s) => write!(f, "{s}"),
            Self::Parameters(SshConnectionParameters { host, port, user }) => {
                write!(f, "ssh://{user}@{host}")?;
                if let Some(port) = port {
                    write!(f, ":{port}")?;
                }
                Ok(())
            }
        }
    }
}

impl SshConnection {
    pub fn connection_string(&self) -> String {
        format!("{self}")
    }
}

#[derive(Debug, Clone)]
pub struct SshTunnelAccess {
    pub connection_string: String,
    pub keypair: SshKey,
}

impl SshTunnelAccess {
    /// Create an ssh tunnel using port fowarding from a random local port to
    /// the host specified in `connection_string`.
    pub async fn create_tunnel<T>(&self, remote_addr: &T) -> Result<(Session, SocketAddr)>
    where
        T: ToSocketAddrs,
    {
        let temp_keyfile = Self::generate_temp_keyfile(self.keypair.to_openssh()?.as_ref()).await?;

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
            .connect(self.connection_string.as_str())
            .await?;

        // Check the status of the connection before proceeding.
        tunnel.check().await?;

        // Find open local port and attempt to create tunnel
        // Retry generating a port up to 10 times
        for _ in 0..10 {
            let local_addr = Self::generate_random_port().await?;

            let local = openssh::Socket::new(&local_addr)?;
            let remote = openssh::Socket::new(remote_addr)?;

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

#[cfg(test)]
mod tests {
    use crate::ssh::{SshConnection, SshConnectionParameters, SshKey, SshTunnelAccess};

    #[tokio::test]
    async fn validate_temp_keyfile() {
        let key = SshKey::generate_random().unwrap();

        let private_key = key.to_openssh().unwrap();

        let temp_keyfile = SshTunnelAccess::generate_temp_keyfile(private_key.as_ref())
            .await
            .unwrap();

        let keyfile_data = std::fs::read(temp_keyfile.path()).unwrap();

        assert_eq!(keyfile_data, private_key.as_bytes());
    }

    #[test]
    fn display_connection_string() {
        let conn_str = SshConnection::ConnectionString("ssh://prod@127.0.0.1:5432".to_string())
            .connection_string();
        assert_eq!(&conn_str, "ssh://prod@127.0.0.1:5432");

        let conn_str = SshConnection::Parameters(SshConnectionParameters {
            host: "127.0.0.1".to_string(),
            port: Some(5432),
            user: "prod".to_string(),
        });
        let conn_str = conn_str.connection_string();
        assert_eq!(&conn_str, "ssh://prod@127.0.0.1:5432");

        // Missing port.
        let conn_str = SshConnection::Parameters(SshConnectionParameters {
            host: "127.0.0.1".to_string(),
            port: None,
            user: "prod".to_string(),
        });
        let conn_str = conn_str.connection_string();
        assert_eq!(&conn_str, "ssh://prod@127.0.0.1");
    }

    #[test]
    fn parse_connection_string() {
        // Valid
        let test_cases = vec![
            (
                "ssh://user@host.com",
                SshConnectionParameters {
                    host: "host.com".to_string(),
                    port: None,
                    user: "user".to_string(),
                },
            ),
            (
                "ssh://user@host.com:1234",
                SshConnectionParameters {
                    host: "host.com".to_string(),
                    port: Some(1234),
                    user: "user".to_string(),
                },
            ),
            (
                "ssh://user@127.0.0.1:1234",
                SshConnectionParameters {
                    host: "127.0.0.1".to_string(),
                    port: Some(1234),
                    user: "user".to_string(),
                },
            ),
        ];
        for (s, v) in test_cases {
            let s: SshConnectionParameters = s.parse().unwrap();
            assert_eq!(s, v);
        }

        // Invalid
        let test_cases = vec![
            "random string",
            "user@host.com",          // doesn't start with `ssh://`
            "ssh://user_at_host.com", // missing `@`
            "ssh://@host.com",        // empty user
            "ssh://user@",            // empty address
            "ssh://user@:1234",       // empty host
            "ssh://host.com:abc",     // invalid port
        ];
        for s in test_cases {
            s.parse::<SshConnectionParameters>()
                .expect_err("invalid ssh connection string should error");
        }
    }
}
