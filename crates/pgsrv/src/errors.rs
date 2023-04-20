use crate::messages::{BackendMessage, FrontendMessage, StartupMessage};
use std::io;

pub type Result<T, E = PgSrvError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum PgSrvError {
    #[error("invalid protocol version: {0}")]
    InvalidProtocolVersion(i32),

    #[error("invalid protocol message length: {0}")]
    InvalidMsgLength(i32),

    #[error("unexpected frontend message: {0:?}")]
    UnexpectedFrontendMessage(Box<FrontendMessage>), // Boxed since frontend message has a large variant.

    #[error("unexpected backend message: {0:?}")]
    UnexpectedBackendMessage(BackendMessage),

    #[error("unexpected startup message: {0:?}")]
    UnexpectedStartupMessage(StartupMessage),

    #[error("message larger than i32 max, size: {0}")]
    MsgTooLarge(usize),

    #[error("missing null byte")]
    MissingNullByte,

    #[error("missing startup parameter: {0}")]
    MissingStartupParameter(&'static str),

    #[error("missing org ID: pass it as an option, or subdomain in proxy or in database as '<org>/<db>'")]
    MissingOrgId,

    #[error("Missing key '{0}' in startup params.")]
    MissingProxyKey(&'static str),

    #[error("Invalid value for key '{key}': {value}")]
    InvalidValueForProxyKey { key: &'static str, value: String },

    /// A stringified error from cloud.
    #[error("cloud: {0}")]
    CloudResponse(String),

    #[error("internal error: {0}")]
    InternalError(String),

    /// We've received an unexpected message identifier from the frontend.
    /// Includes the char representation to allow for easy cross referencing
    /// with the Postgres message format documentation.
    #[error("invalid message type: byte={0}, char={}", *.0 as char)]
    InvalidMsgType(u8),

    #[error("unexpected describe object type: {0}")]
    UnexpectedDescribeObjectType(u8),

    #[error("message length '{0}' exceeds the limit of i32 max")]
    MessageTooLarge(usize),

    #[error(transparent)]
    OpenSsl(#[from] openssl::ssl::Error),

    #[error(transparent)]
    OpenSslStack(#[from] openssl::error::ErrorStack),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    SqlExec(#[from] sqlexec::errors::ExecError),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    PgRepr(#[from] pgrepr::error::PgReprError),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    ReqwestHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
}
