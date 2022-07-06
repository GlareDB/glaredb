use super::keys::{Key, KeySet};
use super::node::coordinator::{AcceptOrCommit, CoordinatorState};
use super::node::replica::ReplicaState;
use super::timestamp::Timestamp;
use super::topology::Address;
use super::transaction::{Transaction, TransactionId};
use super::{AccordError, ComputeData, Executor, NodeId, ReadData, Result, WriteData};
use fmtutil::DisplaySlice;
use log::debug;
use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message<K> {
    pub from: NodeId,
    pub to: Address,
    pub req: u32,
    pub proto_msg: ProtocolMessage<K>,
}

impl<K: fmt::Display> fmt::Display for Message<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "message: (from: {}, to: {}, req: {}, proto: {})",
            self.from, self.to, self.req, self.proto_msg
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreAccept<K> {
    pub tx: Transaction<K>,
}

impl<K: fmt::Display> fmt::Display for PreAccept<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "preaccept: (tx: {})", self.tx,)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreAcceptOk {
    /// Id of the transaction that this message is concerning.
    pub tx: TransactionId,
    /// Proposed timestamp for the transaction. May be the same as the
    /// original timestamp.
    pub proposed: Timestamp,
    /// Transaction dependencies as witnessed by the node.
    pub deps: Vec<TransactionId>,
}

impl fmt::Display for PreAcceptOk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "preaccept ok: (tx: {}, proposed: {}, deps: {})",
            self.tx,
            self.proposed,
            DisplaySlice(&self.deps)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Accept<K> {
    pub tx: Transaction<K>,
    pub timestamp: Timestamp,
    pub deps: Vec<TransactionId>,
}

impl<K: fmt::Display> fmt::Display for Accept<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "accept: (tx: {}, timestamp: {}, deps: {})",
            self.tx,
            self.timestamp,
            DisplaySlice(&self.deps)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AcceptOk {
    pub tx: TransactionId,
    pub deps: Vec<TransactionId>,
}

impl fmt::Display for AcceptOk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "accept ok: (tx: {}, deps: {})",
            self.tx,
            DisplaySlice(&self.deps)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Commit<K> {
    pub tx: Transaction<K>,
    pub timestamp: Timestamp,
    pub deps: Vec<TransactionId>,
}

impl<K: fmt::Display> fmt::Display for Commit<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "commit: (tx: {}, timestamp: {}, deps: {})",
            self.tx,
            self.timestamp,
            DisplaySlice(&self.deps)
        )
    }
}

/// Internal message for the coordinator to begin the execution protocol for a
/// transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartExecuteInternal {
    pub tx: TransactionId,
}

impl fmt::Display for StartExecuteInternal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "start execute internal: (tx: {})", self.tx,)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Read<K> {
    pub tx: Transaction<K>,
    pub timestamp: Timestamp,
    pub deps: Vec<TransactionId>,
}

impl<K: fmt::Display> fmt::Display for Read<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "read: (tx: {}, timestamp: {}, deps: {})",
            self.tx,
            self.timestamp,
            DisplaySlice(&self.deps)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadOk {
    pub tx: TransactionId,
    pub data: ReadData,
}

impl fmt::Display for ReadOk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "read ok: (tx: {})", self.tx)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Apply<K> {
    pub tx: Transaction<K>,
    pub timestamp: Timestamp,
    pub deps: Vec<TransactionId>,
    pub data: ComputeData,
}

impl<K: fmt::Display> fmt::Display for Apply<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "apply: (tx: {}, timestamp: {}, deps: {})",
            self.tx,
            self.timestamp,
            DisplaySlice(&self.deps)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApplyOk {
    pub tx: TransactionId,
    pub data: WriteData,
}

impl fmt::Display for ApplyOk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "apply ok: (tx: {})", self.tx)
    }
}

/// Core protocol messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProtocolMessage<K> {
    Ping,
    Pong,

    /// Begin a read transaction.
    BeginRead {
        keys: KeySet<K>,
        command: Vec<u8>,
    },
    /// Begin a write transaction.
    BeginWrite {
        keys: KeySet<K>,
        command: Vec<u8>,
    },
    // Response for a readonly transaction.
    ReadResponse {
        data: ReadData,
    },
    // Response for a write transaction.
    WriteResponse {
        data: WriteData,
    },

    /// Start the execution protocol on the coordinator side. Should be sent
    /// locally immediately after any commit messages are sent.
    StartExecute(StartExecuteInternal),

    PreAccept(PreAccept<K>),
    PreAcceptOk(PreAcceptOk),
    Accept(Accept<K>),
    AcceptOk(AcceptOk),
    Commit(Commit<K>),
    Read(Read<K>),
    ReadOk(ReadOk),
    Apply(Apply<K>),
    ApplyOk(ApplyOk),
}

impl<K: fmt::Display> fmt::Display for ProtocolMessage<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolMessage::Ping => write!(f, "ping"),
            ProtocolMessage::Pong => write!(f, "pong"),

            ProtocolMessage::BeginRead { keys, .. } => {
                write!(f, "begin read: (keys: {})", keys)
            }
            ProtocolMessage::BeginWrite { keys, .. } => {
                write!(f, "begin write: (keys: {})", keys)
            }

            ProtocolMessage::ReadResponse { .. } => write!(f, "read response"),
            ProtocolMessage::WriteResponse { .. } => write!(f, "write response"),

            ProtocolMessage::StartExecute(msg) => write!(f, "{}", msg),
            ProtocolMessage::PreAccept(msg) => write!(f, "{}", msg),
            ProtocolMessage::PreAcceptOk(msg) => write!(f, "{}", msg),
            ProtocolMessage::Accept(msg) => write!(f, "{}", msg),
            ProtocolMessage::AcceptOk(msg) => write!(f, "{}", msg),
            ProtocolMessage::Commit(msg) => write!(f, "{}", msg),
            ProtocolMessage::Read(msg) => write!(f, "{}", msg),
            ProtocolMessage::ReadOk(msg) => write!(f, "{}", msg),
            ProtocolMessage::Apply(msg) => write!(f, "{}", msg),
            ProtocolMessage::ApplyOk(msg) => write!(f, "{}", msg),
        }
    }
}
