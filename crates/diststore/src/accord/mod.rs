use tokio::sync::mpsc;

mod coordinator;
pub mod keys;
pub mod protocol;
mod replica;
pub mod timestamp;
mod topology;
pub mod transaction;

use std::collections::HashSet;
use timestamp::Timestamp;
use transaction::{Transaction, TransactionId};

/// Globally unique identifier for each node in the system.
pub type NodeId = u64;

#[derive(Debug, thiserror::Error)]
pub enum AccordError {
    #[error("failed to send outbound message: {0}")]
    OutboundSend(String),
    #[error("not enough peers to construct a topology")]
    NotEnoughPeers,
    #[error("fast path electorate is not a subset")]
    ElectorateNotSubset,
    #[error("invalid fast path electorate size: {0}")]
    InvalidElectorateSize(usize),
    #[error("this node is not the transaction coordinator")]
    NodeNotCoordinator,
    #[error("invalid transaction state: {0}")]
    InvalidTransactionState(String),
    #[error("missing transaction: {0}")]
    MissingTx(TransactionId),
    #[error("received proposal from non-voting peer")]
    NonVotingPeer(NodeId),
    #[error("accepted timestamp went backward: have: {have}, accepted: {accepted}")]
    TimestampWentBackward {
        have: Timestamp,
        accepted: Timestamp,
    },
    #[error("internal executor error: {0}")]
    ExecutorError(Box<dyn std::error::Error>),
}

pub type Result<T, E = AccordError> = std::result::Result<T, E>;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReadData {
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ComputeData {
    pub data: Vec<u8>,
}

pub trait Executor<K>: Sync + Send {
    type Error: std::error::Error + 'static;

    /// Execute the read portion of a transaction, the output being fed into
    /// `compute`.
    ///
    /// This must not alter state.
    fn read(&self, ts: &Timestamp, tx: &Transaction<K>) -> Result<ReadData, Self::Error>;

    /// Execute some sort of computation across results, the output being sent
    /// to all peers.
    ///
    /// This must not alter state.
    fn compute(
        &self,
        data: &ReadData,
        ts: &Timestamp,
        tx: &Transaction<K>,
    ) -> Result<ComputeData, Self::Error>;

    /// Write using the the output of `compute`.
    ///
    /// This may alter state, and represents an operation that cannot be ran
    /// concurrently.
    fn write(
        &self,
        data: &ComputeData,
        ts: &Timestamp,
        tx: &Transaction<K>,
    ) -> Result<(), Self::Error>;
}
