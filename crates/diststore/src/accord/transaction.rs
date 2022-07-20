//! Accord transactions.
use super::keys::{Key, KeySet};
use super::timestamp::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Transaction IDs are derived from their original proposed timestamp.
///
/// Generated timestamps are always unique, so it's safe to use this as a
/// globally unique transaction identifier.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TransactionId(pub Timestamp);

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransactionKind {
    Read,
    Write,
}

impl fmt::Display for TransactionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionKind::Read => write!(f, "read"),
            TransactionKind::Write => write!(f, "write"),
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct Transaction<K> {
    id: TransactionId,
    kind: TransactionKind,
    // Keys this transaction is operating on.
    keys: KeySet<K>,
    // Opaque command payload for the transaction.
    command: Vec<u8>,
}

impl<K> Transaction<K> {
    pub const fn new(
        id: TransactionId,
        kind: TransactionKind,
        keys: KeySet<K>,
        command: Vec<u8>,
    ) -> Self {
        Transaction {
            id,
            kind,
            keys,
            command,
        }
    }

    pub fn get_original_ts(&self) -> &Timestamp {
        &self.id.0
    }

    pub fn get_id(&self) -> &TransactionId {
        &self.id
    }

    pub fn get_command(&self) -> &[u8] {
        &self.command
    }

    pub fn is_read_tx(&self) -> bool {
        matches!(self.kind, TransactionKind::Read)
    }

    pub fn is_write_tx(&self) -> bool {
        matches!(self.kind, TransactionKind::Write)
    }
}

impl<K: Key> Transaction<K> {
    /// Check if another transaction conflicts with this one.
    ///
    /// This does not concern itself with the "state" of the transaction (e.g.
    /// if it's been committed). It's on the caller to ensure that only active
    /// transactions are compared.
    pub fn conflicts_with(&self, other: &Self) -> bool {
        if self.id == other.id {
            return false;
        }
        if !self.keys.conflicts_with_any(&other.keys) {
            return false;
        }
        // Only conflicts if one or both transactions are write transactions.
        self.is_write_tx() || other.is_write_tx()
    }
}

impl<K: fmt::Debug> fmt::Debug for Transaction<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .finish()
    }
}

impl<K: fmt::Display> fmt::Display for Transaction<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "id: {}, kind: {}, keys: {}",
            self.id, self.kind, self.keys
        )
    }
}
