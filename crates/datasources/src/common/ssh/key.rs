use ssh_key::{LineEnding, PrivateKey};

#[derive(Debug, thiserror::Error)]
pub enum SshKeyError {
    #[error(transparent)]
    SshKeyGen(#[from] ssh_key::Error),
}

#[derive(Debug, Clone)]
pub struct SshKey {
    keypair: PrivateKey,
}

impl SshKey {
    /// Generate a random Ed25519 ssh key pair.
    pub fn generate_random() -> Result<Self, SshKeyError> {
        let keypair = PrivateKey::random(rand::thread_rng(), ssh_key::Algorithm::Ed25519)?;
        Ok(Self { keypair })
    }

    /// Recreate ssh key from bytes store in catalog.
    pub fn from_bytes(keypair: &[u8]) -> Result<Self, SshKeyError> {
        let keypair = PrivateKey::from_bytes(keypair)?;
        Ok(Self { keypair })
    }

    /// Serialize sshk key as raw bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, SshKeyError> {
        Ok(self.keypair.to_bytes()?.to_vec())
    }

    /// Create an OpenSSH-formatted public key as a String.
    pub fn public_key(&self) -> Result<String, SshKeyError> {
        Ok(self.keypair.public_key().to_openssh()?)
    }

    /// Create an OpenSSH-formatted private key as a String.
    pub(crate) fn to_openssh(&self) -> Result<String, SshKeyError> {
        Ok(self.keypair.to_openssh(LineEnding::default())?.to_string())
    }
}
