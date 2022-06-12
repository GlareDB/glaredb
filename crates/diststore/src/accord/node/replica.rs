use crate::accord::keys::{Key, KeySet};
use crate::accord::protocol::{
    Accept, AcceptOk, Apply, Commit, PreAccept, PreAcceptOk, Read, ReadOk,
};
use crate::accord::timestamp::{Timestamp, TimestampProvider};
use crate::accord::topology::Topology;
use crate::accord::transaction::{Transaction, TransactionId, TransactionKind};
use crate::accord::{AccordError, Executor, NodeId, Result};
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::fmt;

#[derive(Debug)]
pub struct ReplicaState<K> {
    transactions: HashMap<TransactionId, ReplicatedTransaction<K>>,
}

impl<K: Key> ReplicaState<K> {
    pub fn get_node_id(&self) -> NodeId {
        // self.node
        unimplemented!()
    }

    pub fn receive_preaccept(&mut self, msg: PreAccept<K>) -> PreAcceptOk {
        let id = msg.tx.get_id().clone();

        let prop = self.propose_transaction(&msg.tx);
        self.ensure_tx_state(
            msg.tx,
            prop.proposed_timestamp,
            prop.deps,
            TransactionState::PreAccepted,
        );
        let tx = self.transactions.get(&id).unwrap();

        PreAcceptOk {
            tx: id,
            proposed: tx.proposed.clone(),
            deps: tx.deps.iter().cloned().collect(),
        }
    }

    pub fn receive_accept(&mut self, msg: Accept<K>) -> AcceptOk {
        let id = msg.tx.get_id().clone();
        self.ensure_tx_state(msg.tx, msg.timestamp, msg.deps, TransactionState::Accepted);
        let tx = self.transactions.get(&id).unwrap();
        let deps = self.get_deps_with_proposed(tx);
        AcceptOk { tx: id, deps }
    }

    pub fn receive_commit(&mut self, msg: Commit<K>) {
        let id = msg.tx.get_id().clone();
        self.ensure_tx_state(msg.tx, msg.timestamp, msg.deps, TransactionState::Committed);
        let tx = self.transactions.get_mut(&id).unwrap();
        tx.move_to_committed();
    }

    pub fn receive_read(&mut self, msg: Read<K>) -> Result<ReadOk> {
        // TODO: Await committed and applied.
        let id = msg.tx.get_id().clone();
        self.ensure_tx_state(msg.tx, msg.timestamp, msg.deps, TransactionState::Read);
        let tx = self.transactions.get(&id).unwrap();
        unimplemented!()
        // let data = self
        //     .executor
        //     .read(&tx.proposed, &tx.inner)
        //     .map_err(|e| AccordError::ExecutorError(e.to_string()))?;
        // Ok(ReadOk {
        //     tx: tx.inner.get_id().clone(),
        //     data,
        // })
    }

    pub fn receive_apply<E>(&mut self, executor: E, msg: Apply<K>) -> Result<()>
    where
        E: Executor<K>,
    {
        // TODO: Await committed and applied.
        let id = msg.tx.get_id().clone();
        self.ensure_tx_state(msg.tx, msg.timestamp, msg.deps, TransactionState::Applied);
        let tx = self.transactions.get(&id).unwrap();
        executor
            .write(&msg.data, &tx.proposed, &tx.inner)
            .map_err(|e| AccordError::ExecutorError(e.to_string()))?;
        Ok(())
    }

    fn ensure_tx_state(
        &mut self,
        tx: Transaction<K>,
        timestamp: Timestamp,
        deps: Vec<TransactionId>,
        state: TransactionState,
    ) {
        if let Some(tx) = self.transactions.get_mut(tx.get_id()) {
            if timestamp > tx.proposed {
                tx.proposed = timestamp
            }
            tx.merge_deps(deps);
            tx.move_to_state(state);
        } else {
            let id = tx.get_id().clone();
            let tx = ReplicatedTransaction::new(tx, timestamp, deps);
            self.transactions.insert(id.clone(), tx);
        }
    }

    /// Check if the given transaction has any dependencies, and if it does,
    /// return a proposal with the dependencies and a timestamp that's greater
    /// than the timestamp for any dependency.
    ///
    /// If there are no dependencies, `None` is returned.
    ///
    /// Transaction 'a' is a dependency of transaction 'b' if 'a' conflicts with
    /// 'b', the original timestamp of 'a' is less than original timestamp of
    /// 'b', and if 'a' has not yet been committed.
    fn propose_transaction(&self, tx: &Transaction<K>) -> Proposal {
        let mut iter = self.transactions.iter().filter(|(_, other)| {
            let conflicts = other.inner.conflicts_with(tx);
            let earlier = other.get_original() < &tx.get_id().0;
            let stable = other.is_committed_or_applied();
            conflicts && earlier && !stable
        });

        let (mut deps, mut max) = match iter.next() {
            Some((id, other)) => (vec![id.clone()], tx.get_original_ts().max(&other.proposed)),
            None => {
                // No conflicts, use the original timestamp as the proposed.
                return Proposal {
                    deps: Vec::new(),
                    proposed_timestamp: tx.get_original_ts().clone(),
                };
            }
        };

        for (id, other) in iter {
            deps.push(id.clone());
            max = max.max(&other.proposed);
        }

        unimplemented!()
        // Proposal {
        //     deps,
        //     proposed_timestamp: max.next_logical(self.node),
        // }
    }

    /// Get all dependencies for a replicated transaction.
    ///
    /// A transaction is a dependency if its original timestamp is less than the
    /// _proposed_ timestamp of the replicated transaction we're checking.
    ///
    /// Check the proposed timestamp is what makes this differ from dependency
    /// checking in `propose_transaction` where we're only looking at original
    /// timestamps.
    fn get_deps_with_proposed(&self, tx: &ReplicatedTransaction<K>) -> Vec<TransactionId> {
        self.transactions
            .iter()
            .filter(|(_, other)| {
                let conflicts = other.inner.conflicts_with(&tx.inner);
                let earlier = other.get_original() < &tx.proposed;
                let stable = other.is_committed_or_applied();
                conflicts && earlier && !stable
            })
            .map(|(id, _)| id.clone())
            .collect()
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

#[derive(Debug)]
enum TransactionState {
    PreAccepted,
    Accepted,
    Committed,
    Read,
    Applied,
}

impl fmt::Display for TransactionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransactionState::*;
        write!(
            f,
            "{}",
            match self {
                PreAccepted => "preaccepted",
                Accepted => "accepted",
                Committed => "committed",
                Read => "read",
                Applied => "applied",
            }
        )
    }
}

#[derive(Debug)]
struct ReplicatedTransaction<K> {
    inner: Transaction<K>,
    state: TransactionState,
    proposed: Timestamp,
    deps: HashSet<TransactionId>,
}

impl<K: Key> ReplicatedTransaction<K> {
    fn new(tx: Transaction<K>, proposed: Timestamp, deps: Vec<TransactionId>) -> Self {
        ReplicatedTransaction {
            inner: tx,
            state: TransactionState::PreAccepted,
            proposed,
            deps: deps.into_iter().collect(),
        }
    }

    fn get_original(&self) -> &Timestamp {
        &self.inner.get_id().0
    }

    fn is_committed_or_applied(&self) -> bool {
        match self.state {
            TransactionState::Committed | TransactionState::Applied => true,
            _ => false,
        }
    }

    fn move_to_state(&mut self, state: TransactionState) {
        match state {
            TransactionState::Accepted => self.move_to_accepted(),
            TransactionState::Committed => self.move_to_committed(),
            TransactionState::Applied => self.move_to_applied(),
            TransactionState::Read => self.move_to_read(),
            other => warn!("attempt to move to strange state: {}", other),
        }
    }

    fn move_to_accepted(&mut self) {
        match self.state {
            TransactionState::PreAccepted => self.state = TransactionState::Accepted,
            ref other => info!("ignoring 'accepted' state update, current: {}", other),
        }
    }

    fn move_to_committed(&mut self) {
        match self.state {
            TransactionState::PreAccepted | TransactionState::Accepted => {
                self.state = TransactionState::Committed
            }
            ref other => info!("ignoring 'committed' state update, current: {}", other),
        }
    }

    fn move_to_read(&mut self) {
        match self.state {
            TransactionState::PreAccepted
            | TransactionState::Accepted
            | TransactionState::Committed => self.state = TransactionState::Read,
            ref other => info!("ignoring 'read' state update, current: {}", other),
        }
    }

    fn move_to_applied(&mut self) {
        match self.state {
            TransactionState::PreAccepted
            | TransactionState::Accepted
            | TransactionState::Committed
            | TransactionState::Read => self.state = TransactionState::Applied,
            ref other => info!("ignoring 'applied' state update, current: {}", other),
        }
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
