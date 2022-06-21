use super::keys::{Key, KeySet};
use super::node::coordinator::{AcceptOrCommit, CoordinatorState};
use super::node::replica::ReplicaState;
use super::timestamp::Timestamp;
use super::topology::Address;
use super::transaction::{Transaction, TransactionId};
use super::{AccordError, ComputeData, Executor, NodeId, ReadData, Result, WriteData};
use log::debug;
use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message<K> {
    pub from: NodeId,
    pub to: Address,
    pub proto_msg: ProtocolMessage<K>,
}

/// Messages sent to peers in the cluster.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeerMessage<K> {
    pub from: NodeId,
    pub to: Address,
    pub proto_msg: ProtocolMessage<K>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreAccept<K> {
    pub tx: Transaction<K>,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Accept<K> {
    pub tx: Transaction<K>,
    pub timestamp: Timestamp,
    pub deps: Vec<TransactionId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AcceptOk {
    pub tx: TransactionId,
    pub deps: Vec<TransactionId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Commit<K> {
    pub tx: Transaction<K>,
    pub timestamp: Timestamp,
    pub deps: Vec<TransactionId>,
}

/// Internal message for the coordinator to begin the execution protocol for a
/// transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartExecuteInternal {
    pub tx: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Read<K> {
    pub tx: Transaction<K>,
    pub timestamp: Timestamp,
    pub deps: Vec<TransactionId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadOk {
    pub tx: TransactionId,
    pub data: ReadData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Apply<K> {
    pub tx: Transaction<K>,
    pub timestamp: Timestamp,
    pub deps: Vec<TransactionId>,
    pub data: ComputeData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApplyOk {
    pub tx: TransactionId,
    pub data: WriteData,
}

/// Core protocol messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProtocolMessage<K> {
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

pub enum ClientMessage<K> {
    BeginRead { keys: KeySet<K> },
}
