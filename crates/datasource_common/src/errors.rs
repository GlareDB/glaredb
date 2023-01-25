#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    OpenSsh(#[from] openssh::Error),

    #[error(transparent)]
    SshKeyGen(#[from] ssh_key::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Cannot establish SSH tunnel: {0}")]
    SshPortForward(openssh::Error),

    #[error("Failed to find an open port to open the SSH tunnel")]
    NoOpenPorts,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
