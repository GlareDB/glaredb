use openraft::error::RPCError;

use crate::repr::{Node, NodeId};

#[derive(thiserror::Error)]
pub enum Error {}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type RpcError<T> = RPCError<NodeId, Node, T>;
pub type RpcResult<T, E> = std::result::Result<T, RpcError<E>>;
