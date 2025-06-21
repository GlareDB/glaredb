use std::error::Error;
use std::fmt;

use glaredb_error::DbError;
use napi::Error as NapiError;

pub type Result<T, E = NodeError> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum NodeError {
    Glaredb(DbError),
    Napi(NapiError),
}

impl Error for NodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Glaredb(err) => err.source(),
            Self::Napi(err) => err.source(),
        }
    }
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Glaredb(err) => err.fmt(f),
            Self::Napi(err) => err.fmt(f),
        }
    }
}

impl From<DbError> for NodeError {
    fn from(value: DbError) -> Self {
        NodeError::Glaredb(value)
    }
}

impl From<NapiError> for NodeError {
    fn from(value: NapiError) -> Self {
        NodeError::Napi(value)
    }
}

impl From<NodeError> for NapiError {
    fn from(value: NodeError) -> Self {
        match value {
            NodeError::Glaredb(error) => NapiError::from_reason(error.to_string()),
            NodeError::Napi(error) => error,
        }
    }
}
