use crate::messages::{FrontendMessage, StartupMessage};
use std::io;

pub type Result<T, E = PgSrvError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum PgSrvError {
    #[error("invalid protocol version: {0}")]
    InvalidProtocolVersion(i32),

    #[error("unexpected frontend message: {0:?}")]
    UnexpectedFrontendMessage(FrontendMessage),

    #[error("unexpected startup message: {0:?}")]
    UnexpectedStartupMessage(StartupMessage),

    #[error("message larger than i32 max, size: {0}")]
    MsgTooLarge(usize),

    #[error("missing null byte")]
    MissingNullByte,

    /// We've received an unexpected message identifier from the frontend.
    /// Includes the char representation to allow for easy cross referencing
    /// with the Postgres message format documentation.
    #[error("invalid message type: byte={0}, char={}", *.0 as char)]
    InvalidMsgType(u8),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    SqlExec(#[from] sqlexec::errors::ExecError),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
}
