use super::keys::{Key, KeySet};
use super::timestamp::TimestampProvider;
use super::transaction::{Transaction, TransactionId, TransactionKind};
use super::NodeId;
use std::collections::HashMap;

#[derive(Debug)]
pub struct ReplicaState<K> {
    /// Transactions created by this replica that are currently active.
    inflight_transactions: HashMap<TransactionId, Transaction<K>>,
    ts_provider: TimestampProvider,
}

impl<K: Key> ReplicaState<K> {
    /// Create a new transaction, and return a copy suitable for sending to
    /// other nodes.
    pub fn new_inflight_tx(
        &mut self,
        kind: TransactionKind,
        keys: KeySet<K>,
        command: Vec<u8>,
    ) -> Transaction<K> {
        let ts = self.ts_provider.unique_now();
        let id = TransactionId(ts);
        let tx = Transaction::new(id.clone(), kind, keys, command);
        self.inflight_transactions.insert(id, tx.clone());
        tx
    }
}
