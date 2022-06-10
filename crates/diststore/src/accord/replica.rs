use super::keys::{Key, KeySet};
use super::timestamp::{Timestamp, TimestampProvider};
use super::topology::Topology;
use super::transaction::{Transaction, TransactionId, TransactionKind};
use super::NodeId;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct ReplicaState<K> {
    node: NodeId,
    topology: Topology,
    tracker: Tracker<K>,
    ts_provider: TimestampProvider,
}

impl<K: Key> ReplicaState<K> {
    pub fn get_node_id(&self) -> NodeId {
        self.node
    }

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
        self.tracker.track_local_tx(tx.clone());
        tx
    }

    /// PreAccept a remote transaction, proposing a new timestamp if necessary.
    pub fn preaccept_tx(&mut self, tx: Transaction<K>) -> Option<Proposal> {
        let prop = self.propose_transaction(&tx);
        self.tracker.track_remote_tx(tx, prop.clone());
        prop
    }

    pub fn preaccept_proposal_from(&mut self, node: NodeId, proposal: Proposal) {}

    /// Check if the given transaction has any dependencies, and if it does,
    /// return a proposal with the dependencies and a timestamp that's greater
    /// than the timestamp for any dependency.
    ///
    /// If there are no dependencies, `None` is returned.
    ///
    /// Transaction 'a' is a dependency of transaction 'b' if 'a' conflicts with
    /// 'b' and the original timestamp of 'a' is less than original timestamp of
    /// 'b'.
    fn propose_transaction(&self, tx: &Transaction<K>) -> Option<Proposal> {
        let mut iter = self.tracker.iter().filter(|other| {
            let later = other.get_original() < &tx.get_id().0;
            let conflicts = tx.conflicts_with(&other.inner);
            later && conflicts
        });

        let mut prop = match iter.next() {
            Some(tx) => Proposal {
                deps: vec![tx.inner.get_id().clone()],
                proposed_timestamp: tx.get_current().clone(),
            },
            None => return None,
        };

        for tx in self.tracker.iter() {
            prop.deps.push(tx.inner.get_id().clone());
            if tx.get_current() > &prop.proposed_timestamp {
                prop.proposed_timestamp = tx.get_current().clone();
            }
        }

        prop.proposed_timestamp = prop.proposed_timestamp.next_logical(self.node);

        Some(prop)
    }
}

#[derive(Debug, Clone)]
pub struct Proposal {
    /// A list of dependencies that must commit before a given transaction can
    /// commit.
    pub deps: Vec<TransactionId>,
    /// A timestamp that greater than any timestamp in the list of dependencies.
    pub proposed_timestamp: Timestamp,
}

#[derive(Debug, Clone, PartialEq)]
enum TransactionStatus {
    /// Status immediately set for transactions created by this node.
    Created,
    /// Corresponds to "preaccept" phase in accord.
    PreAccepted {
        /// Max timestamp we've received so far.
        max_proposal: Option<Timestamp>,
        /// Union of all deps received from all nodes.
        deps: HashSet<TransactionId>,
        /// Nodes that we've received messages from.
        recieved_from: HashSet<NodeId>,
    },
    /// Corresponds to "accept" phase in accord.
    Accepted {
        /// Timestamp we will be committing with.
        proposed: Timestamp,
        /// Union of all deps.
        deps: HashSet<TransactionId>,
    },
    /// Corresponds to "commit" phase in accord.
    Committed {
        /// Timestamp we will be applying with.
        proposed: Timestamp,
        /// Union of all deps.
        deps: HashSet<TransactionId>,
    },
    /// Corresponds to "apply" phase in accord.
    Applied {
        /// Timestamp we have applied with.
        proposed: Timestamp,
        /// Union of all deps.
        deps: HashSet<TransactionId>,
    },
}

/// Source of the transaction.
#[derive(Debug)]
enum TransactionSource {
    /// Transaction created on this node.
    Local,
    /// Transaction received from another node.
    Peer,
}

/// A transaction wrapped with some additional info.
#[derive(Debug)]
struct TransactionState<K> {
    inner: Transaction<K>,
    /// Current transaction status.
    status: TransactionStatus,
    /// Where this transaction came from.
    source: TransactionSource,
}

impl<K> TransactionState<K> {
    fn get_original(&self) -> &Timestamp {
        &self.inner.get_id().0
    }

    /// Get either the current max proposed timestamp, or the original if
    /// there's no proposed timestamp.
    fn get_current(&self) -> &Timestamp {
        match &self.status {
            TransactionStatus::Created => self.get_original(),
            TransactionStatus::PreAccepted { max_proposal, .. } => {
                max_proposal.as_ref().unwrap_or(self.get_original())
            }
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
struct Tracker<K> {
    /// All transactions, both those being coordinated by this node, as well as
    /// transactions received from other nodes.
    transactions: HashMap<TransactionId, TransactionState<K>>,
}

impl<K: Key> Tracker<K> {
    fn track_local_tx(&mut self, tx: Transaction<K>) {
        let id = tx.get_id().clone();
        // let wrapped = TransactionState {
        //     inner: tx,
        //     proposed: None,
        //     status: TransactionStatus::Created,
        //     source: TransactionSource::Local,
        //     deps: Vec::new(),
        // };
        // self.transactions.insert(id, wrapped);
    }

    fn track_remote_tx(&mut self, tx: Transaction<K>, proposal: Option<Proposal>) {
        let id = tx.get_id().clone();
        let (proposed, deps) = match proposal {
            Some(prop) => (Some(prop.proposed_timestamp), prop.deps),
            None => (None, Vec::new()),
        };
        // let wrapped = TransactionState {
        //     inner: tx,
        //     proposed,
        //     status: TransactionStatus::PreAccepted,
        //     source: TransactionSource::Peer,
        //     deps,
        // };
        // self.transactions.insert(id, wrapped);
    }

    fn iter(&self) -> impl Iterator<Item = &TransactionState<K>> {
        self.transactions.iter().map(|(_, tx)| tx)
    }
}
