#[derive(Debug, thiserror::Error)]
pub enum DatasourceCommonError {
    /// Generic openssh errors.
    ///
    /// Using debug to get the underlying errors (the openssh crate doesn't keep
    /// those in the message).
    #[error("{0:?}")]
    OpenSsh(#[from] openssh::Error),

    #[error(transparent)]
    SshKeyGen(#[from] ssh_key::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Port forward error with openssh.
    ///
    /// Using debug to get the underlying errors (the openssh crate doesn't keep
    /// those in the message).
    #[error("Cannot establish SSH tunnel: {0:?}")]
    SshPortForward(openssh::Error),

    #[error("Failed to find an open port to open the SSH tunnel")]
    NoOpenPorts,

    #[error("Invalid SSH connection string: {0}")]
    SshConnectionParseError(String),

    #[error("Feature currently unsupported: {0}")]
    Unsupported(&'static str),

    #[error(transparent)]
    ListingErrBoxed(#[from] Box<dyn std::error::Error + Sync + Send>),

    #[error("Scalar of type '{0}' not supported")]
    UnsupportedDatafusionScalar(datafusion::arrow::datatypes::DataType),

    #[error(transparent)]
    ReprError(#[from] repr::error::ReprError),

    #[error(transparent)]
    FmtError(#[from] core::fmt::Error),
}

pub type Result<T, E = DatasourceCommonError> = std::result::Result<T, E>;
