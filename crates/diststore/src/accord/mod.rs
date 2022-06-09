use tokio::sync::mpsc;

pub mod keys;
pub mod protocol;
mod replica;
pub mod timestamp;
pub mod transaction;

/// Globally unique identifier for each node in the system.
pub type NodeId = u64;

#[derive(Debug, thiserror::Error)]
pub enum AccordError {
    #[error("failed to send outbound message: {0}")]
    OutboundSend(String),
}

pub type Result<T, E = AccordError> = std::result::Result<T, E>;
