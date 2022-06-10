use tokio::sync::mpsc;

pub mod keys;
pub mod protocol;
mod replica;
pub mod timestamp;
mod topology;
pub mod transaction;

/// Globally unique identifier for each node in the system.
pub type NodeId = u64;

#[derive(Debug, thiserror::Error)]
pub enum AccordError {
    #[error("failed to send outbound message: {0}")]
    OutboundSend(String),
    #[error("not enough peers to construct a topology")]
    NotEnoughPeers,
    #[error("this node is not the transaction coordinator")]
    NodeNotCoordinator,
    #[error("invalid transaction state: {0}")]
    InvalidTransactionState(String),
    #[error("missing transaction: {0}")]
    MissingTx(transaction::TransactionId),
}

pub type Result<T, E = AccordError> = std::result::Result<T, E>;
