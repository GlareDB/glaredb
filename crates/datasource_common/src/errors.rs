#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    OpenSsh(#[from] openssh::Error),

    #[error(transparent)]
    SshKeyGen(#[from] ssh_key::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::Error::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
