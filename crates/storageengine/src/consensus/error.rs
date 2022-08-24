use openraft::error::RPCError;

use super::{GlareNode, GlareNodeId};

#[derive(thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    ToyRpcError(#[from] toy_rpc::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type RpcError<T> = RPCError<GlareNodeId, GlareNode, T>;
pub type RpcResult<T, E> = std::result::Result<T, RpcError<E>>;
