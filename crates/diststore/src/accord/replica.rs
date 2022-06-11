use super::keys::{Key, KeySet};
use super::timestamp::{Timestamp, TimestampProvider};
use super::topology::Topology;
use super::transaction::{Transaction, TransactionId, TransactionKind};
use super::{AccordError, NodeId, Result};
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
        self.tracker.track_coordinating_tx(tx.clone());
        tx
    }

    /// PreAccept a remote transaction, proposing a new timestamp if necessary.
    pub fn preaccept_tx(&mut self, tx: Transaction<K>) -> Option<Proposal> {
        let prop = self.propose_transaction(&tx);
        self.tracker.track_preaccepted_tx(tx, prop.clone());
        prop
    }

    /// Preaccept a proposal from a node.
    ///
    /// If a fast path has been reached, a commit message will be returned. If
    /// a simple quorum has been reached, an accept message with the highest
    /// timestamp will be returned. Otherwise no messages will be returned.
    pub fn coord_preaccept_proposal(
        &mut self,
        node: NodeId,
        tx: &TransactionId,
        proposal: Proposal,
    ) -> Result<Option<CommitOrAccept>> {
        let tx = self
            .tracker
            .get_tx_mut(tx)
            .ok_or(AccordError::MissingTx(tx.clone()))?;
        let received_from = tx.merge_preaccept_proposal(node, proposal)?;

        let check = self.topology.check_quorum(received_from);

        // No shard has proposed a higher timestamp yet, check if fast path
        // available.
        if tx.get_original() == &tx.proposed {
            if check.have_fast_path {
                // We have fast path, send out commit.
                return Ok(Some(CommitOrAccept::Commit {
                    timestamp: tx.proposed.clone(),
                    deps: tx.deps.iter().cloned().collect(),
                }));
            }
            // Wait until we have more responses to either.
            return Ok(None);
        }

        // Tx has a different proposed timestamp than its original. Use that
        // and send out to all shards if we have a simple quorum.
        if check.have_slow_path {
            return Ok(Some(CommitOrAccept::Accept {
                timestamp: tx.proposed.clone(),
                deps: tx.deps.iter().cloned().collect(),
            }));
        }

        // No quorum reached yet.
        return Ok(None);
    }

    /// Accept the coordinator's timestamp, returning a full set of dependencies
    /// that this node has witnessed.
    pub fn accept(
        &mut self,
        tx: &TransactionId,
        timestamp: Timestamp,
        deps: Vec<TransactionId>,
    ) -> Result<Vec<TransactionId>> {
        let tx = self
            .tracker
            .get_tx_mut(tx)
            .ok_or(AccordError::MissingTx(tx.clone()))?;
        tx.accept(timestamp, deps)?;
        Ok(tx.deps.iter().cloned().collect())
    }

    pub fn coord_accept_ok(
        &mut self,
        tx: &TransactionId,
        deps: Vec<TransactionId>,
    ) -> Result<Vec<TransactionId>> {
        unimplemented!()
    }

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
                proposed_timestamp: tx.proposed.clone(),
            },
            None => return None,
        };

        for tx in self.tracker.iter() {
            prop.deps.push(tx.inner.get_id().clone());
            if tx.proposed > prop.proposed_timestamp {
                prop.proposed_timestamp = tx.proposed.clone();
            }
        }

        prop.proposed_timestamp = prop.proposed_timestamp.next_logical(self.node);

        Some(prop)
    }
}

#[derive(Debug)]
pub enum CommitOrAccept {
    Commit {
        timestamp: Timestamp,
        deps: Vec<TransactionId>,
    },
    Accept {
        timestamp: Timestamp,
        deps: Vec<TransactionId>,
    },
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
    PreAccepted,
    /// Corresponds to "accept" phase in accord.
    Accepted,
    /// Corresponds to "commit" phase in accord.
    Committed,
    /// Corresponds to "apply" phase in accord.
    Applied,
}

/// Extra state that the coordinator needs to hold.
#[derive(Debug)]
enum CoordinatingState {
    PreAcceptOk { received_from: HashSet<NodeId> },
    AcceptOk { received_from: HashSet<NodeId> },
}

/// Source of the transaction.
#[derive(Debug)]
enum TransactionSource {
    /// Transaction created on this node. This node is coordinating.
    Local(CoordinatingState),
    /// Transaction received from another node.
    Peer,
}

impl TransactionSource {
    fn insert_for_preaccept(&mut self, node: NodeId) -> Result<()> {
        match self {
            Self::Local(CoordinatingState::PreAcceptOk { received_from }) => {
                received_from.insert(node);
                Ok(())
            }
            other => Err(AccordError::InvalidTransactionState(format!("{:?}", other))),
        }
    }

    fn get_for_preaccept(&self) -> Result<&HashSet<NodeId>> {
        match self {
            Self::Local(CoordinatingState::PreAcceptOk { received_from }) => Ok(received_from),
            other => Err(AccordError::InvalidTransactionState(format!("{:?}", other))),
        }
    }

    fn insert_for_accept(&mut self, node: NodeId) -> Result<()> {
        match self {
            Self::Local(CoordinatingState::AcceptOk { received_from }) => {
                received_from.insert(node);
                Ok(())
            }
            other => Err(AccordError::InvalidTransactionState(format!("{:?}", other))),
        }
    }

    fn get_for_accept(&self) -> Result<&HashSet<NodeId>> {
        match self {
            Self::Local(CoordinatingState::AcceptOk { received_from }) => Ok(received_from),
            other => Err(AccordError::InvalidTransactionState(format!("{:?}", other))),
        }
    }
}

/// A transaction wrapped with some additional info.
#[derive(Debug)]
struct TransactionState<K> {
    inner: Transaction<K>,
    /// Proposed timestamp. Initially set to the original timestamp.
    proposed: Timestamp,
    /// Current transaction status.
    status: TransactionStatus,
    /// Where this transaction came from.
    source: TransactionSource,
    /// Union of all deps received from all nodes.
    deps: HashSet<TransactionId>,
}

impl<K: Key> TransactionState<K> {
    fn get_original(&self) -> &Timestamp {
        &self.inner.get_id().0
    }

    /// Merge another node's proposal into this transaction state, returning
    /// the entire set of nodes that we've received proposals from.
    ///
    /// "This" node must be coordinating the transaction, and the transaction
    /// must be in the "preaccept" phase.
    fn merge_preaccept_proposal(
        &mut self,
        from: NodeId,
        prop: Proposal,
    ) -> Result<&HashSet<NodeId>> {
        self.source.insert_for_preaccept(from)?;
        self.merge_deps(prop.deps);
        if prop.proposed_timestamp > self.proposed {
            self.proposed = prop.proposed_timestamp;
        }
        self.source.get_for_preaccept()
    }

    /// Move this transaction to "accepted", setting the proposed timestamp and
    /// merging all dependencies.
    fn accept(&mut self, timestamp: Timestamp, deps: Vec<TransactionId>) -> Result<()> {
        if timestamp < self.proposed {
            return Err(AccordError::TimestampWentBackward {
                have: self.proposed.clone(),
                accepted: timestamp,
            });
        }
        self.status = TransactionStatus::Accepted;
        self.proposed = timestamp;
        self.merge_deps(deps);
        Ok(())
    }

    fn merge_deps<I>(&mut self, deps: I)
    where
        I: IntoIterator<Item = TransactionId>,
    {
        for dep in deps.into_iter() {
            self.deps.insert(dep);
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
    fn track_coordinating_tx(&mut self, tx: Transaction<K>) {
        let id = tx.get_id().clone();
        let state = TransactionState {
            inner: tx,
            proposed: id.0.clone(),
            status: TransactionStatus::Created,
            source: TransactionSource::Local(CoordinatingState::PreAcceptOk {
                received_from: HashSet::new(),
            }),
            deps: HashSet::new(),
        };
        self.transactions.insert(id, state);
    }

    /// Track a preaccepted transaction from a peer, and store this node's
    /// proposal.
    fn track_preaccepted_tx(&mut self, tx: Transaction<K>, proposal: Option<Proposal>) {
        let id = tx.get_id().clone();
        let (proposed, deps) = match proposal {
            Some(prop) => (prop.proposed_timestamp, prop.deps.into_iter().collect()),
            None => (id.0.clone(), HashSet::new()),
        };
        let state = TransactionState {
            inner: tx,
            proposed,
            status: TransactionStatus::PreAccepted,
            source: TransactionSource::Peer,
            deps,
        };
        self.transactions.insert(id, state);
    }

    fn get_tx(&self, id: &TransactionId) -> Option<&TransactionState<K>> {
        self.transactions.get(id)
    }

    fn get_tx_mut(&mut self, id: &TransactionId) -> Option<&mut TransactionState<K>> {
        self.transactions.get_mut(id)
    }

    fn iter(&self) -> impl Iterator<Item = &TransactionState<K>> {
        self.transactions.iter().map(|(_, tx)| tx)
    }
}
