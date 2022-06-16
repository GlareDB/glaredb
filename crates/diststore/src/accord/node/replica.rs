use crate::accord::keys::{Key, KeySet};
use crate::accord::log::Log;
use crate::accord::protocol::{
    Accept, AcceptOk, Apply, ApplyOk, Commit, PreAccept, PreAcceptOk, Read, ReadOk,
};
use crate::accord::timestamp::{Timestamp, TimestampProvider};
use crate::accord::topology::Topology;
use crate::accord::transaction::{Transaction, TransactionId, TransactionKind};
use crate::accord::{AccordError, ComputeData, Executor, NodeId, Result};
use log::{debug, info, warn};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

#[derive(Debug)]
pub struct ReplicaState<K> {
    node: NodeId,
    log: Log,
    /// Transactions that need to await for their dependencies to commit and
    /// apply.
    blocked: BlockedTransactions,
    transactions: HashMap<TransactionId, ReplicatedTransaction<K>>,
}

impl<K: Key> ReplicaState<K> {
    pub fn new(node: NodeId, log: Log) -> Self {
        ReplicaState {
            node,
            log,
            blocked: BlockedTransactions::new(),
            transactions: HashMap::new(),
        }
    }

    pub fn get_node_id(&self) -> NodeId {
        self.node
    }

    pub fn receive_preaccept(&mut self, msg: PreAccept<K>) -> PreAcceptOk {
        debug!("received preaccept: {:?}", msg);
        let id = msg.tx.get_id().clone();

        let prop = self.propose_transaction(&msg.tx);
        debug!("transaction proposal: {:?}", prop);
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
        debug!("received accept: {:?}", msg);
        let id = msg.tx.get_id().clone();
        self.ensure_tx_state(msg.tx, msg.timestamp, msg.deps, TransactionState::Accepted);
        let tx = self.transactions.get(&id).unwrap();
        let deps = self.get_deps_with_proposed(tx);
        AcceptOk { tx: id, deps }
    }

    pub fn receive_commit(&mut self, msg: Commit<K>) -> Result<()> {
        debug!("received commit: {:?}", msg);
        let id = msg.tx.get_id().clone();
        self.ensure_tx_state(msg.tx, msg.timestamp, msg.deps, TransactionState::Durable);
        let tx = self.transactions.get_mut(&id).unwrap();
        self.log.write_committed(&tx.inner, &tx.proposed)
    }

    pub fn receive_read<E>(&mut self, executor: E, msg: Read<K>) -> Result<Vec<ExecutionActionOk>>
    where
        E: Executor<K>,
    {
        debug!("received read: {:?}", msg);
        let id = msg.tx.get_id().clone();
        self.ensure_tx_state(msg.tx, msg.timestamp, msg.deps, TransactionState::Durable);

        self.blocked.insert(id, BlockedAction::Read);
        self.blocked
            .drain_execute(executor, &mut self.log, &self.transactions)
    }

    pub fn receive_apply<E>(&mut self, executor: E, msg: Apply<K>) -> Result<Vec<ExecutionActionOk>>
    where
        E: Executor<K>,
    {
        debug!("received apply: {:?}", msg);
        let id = msg.tx.get_id().clone();
        self.ensure_tx_state(msg.tx, msg.timestamp, msg.deps, TransactionState::Durable);

        self.blocked.insert(id, BlockedAction::Apply(msg.data));
        self.blocked
            .drain_execute(executor, &mut self.log, &self.transactions)
    }

    pub fn try_execute_blocked<E>(&mut self, executor: E) -> Result<Vec<ExecutionActionOk>>
    where
        E: Executor<K>,
    {
        self.blocked
            .drain_execute(executor, &mut self.log, &self.transactions)
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
    /// 'b'.
    fn propose_transaction(&self, tx: &Transaction<K>) -> Proposal {
        let mut iter = self.transactions.iter().filter(|(_, other)| {
            let conflicts = other.inner.conflicts_with(tx);
            let earlier = other.get_original() < &tx.get_id().0;
            conflicts && earlier
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

        Proposal {
            deps,
            proposed_timestamp: max.next_logical(self.node),
        }
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
                conflicts && earlier
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
    /// The "commit" and "applied" phases are both recorded to the log.
    Durable,
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
                Durable => "durable",
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

    fn is_durable(&self) -> bool {
        match self.state {
            TransactionState::Durable => true,
            _ => false,
        }
    }

    fn move_to_state(&mut self, state: TransactionState) {
        match state {
            TransactionState::Accepted => self.move_to_accepted(),
            TransactionState::Durable => self.move_to_durable(),
            other => warn!("attempt to move to strange state: {}", other),
        }
    }

    fn move_to_accepted(&mut self) {
        match self.state {
            TransactionState::PreAccepted => self.state = TransactionState::Accepted,
            ref other => info!("ignoring 'accepted' state update, current: {}", other),
        }
    }

    fn move_to_durable(&mut self) {
        match self.state {
            TransactionState::PreAccepted | TransactionState::Accepted => {
                self.state = TransactionState::Durable
            }
            ref other => info!("ignoring 'durable' state update, current: {}", other),
        }
    }

    /// Whether or not this transaction can continue with execution.
    ///
    /// Ensures that all transactions that this transaction depends on have been
    /// both durably committed and applied according to the log.
    fn can_continue_execution(
        &self,
        log: &Log,
        transactions: &HashMap<TransactionId, ReplicatedTransaction<K>>,
    ) -> Result<bool> {
        let committed_ts = log.get_latest_commit_ts();
        let applied_ts = log.get_latest_applied_ts();

        for dep in self.deps.iter() {
            let dep = transactions
                .get(dep)
                .ok_or(AccordError::MissingTx(dep.clone()))?;

            if &dep.proposed > committed_ts || &dep.proposed > applied_ts {
                return Ok(false);
            }
        }

        Ok(true)
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

/// Results of execution either a "read" or "apply".
#[derive(Debug, PartialEq)]
pub enum ExecutionActionOk {
    ReadOk(ReadOk),
    ApplyOk(ApplyOk),
}

impl ExecutionActionOk {
    fn get_tx_id(&self) -> &TransactionId {
        match self {
            ExecutionActionOk::ReadOk(msg) => &msg.tx,
            ExecutionActionOk::ApplyOk(msg) => &msg.tx,
        }
    }

    fn unwrap_read_ok(self) -> ReadOk {
        match self {
            ExecutionActionOk::ReadOk(msg) => msg,
            action => panic!("exection action not 'read ok': {:?}", action),
        }
    }

    fn unwrap_apply_ok(self) -> ApplyOk {
        match self {
            ExecutionActionOk::ApplyOk(msg) => msg,
            action => panic!("exection action not 'apply ok': {:?}", action),
        }
    }
}

#[derive(Debug)]
enum BlockedAction {
    Read,
    Apply(ComputeData),
}

#[derive(Debug)]
struct BlockedTransactions {
    /// Transactions currently waiting for dependencies to commit and apply.
    ///
    /// Note that this is a btree so we can iterator in order of original
    /// timestamps. This allows us to more easily assert number and order of
    /// executions in tests for each call to `drain_execute`.
    transactions: BTreeMap<TransactionId, BlockedAction>,
}

impl BlockedTransactions {
    fn new() -> BlockedTransactions {
        BlockedTransactions {
            transactions: BTreeMap::new(),
        }
    }

    fn insert(&mut self, id: TransactionId, action: BlockedAction) {
        match self.transactions.insert(id, action) {
            Some(prev) => info!("new action superseding old action {:?}", prev),
            None => (),
        }
    }

    fn drain_execute<K, E>(
        &mut self,
        executor: E,
        log: &mut Log,
        transactions: &HashMap<TransactionId, ReplicatedTransaction<K>>,
    ) -> Result<Vec<ExecutionActionOk>>
    where
        K: Key,
        E: Executor<K>,
    {
        let mut to_remove = Vec::new();
        let mut results = Vec::new();

        // TODO: Use hashmap's drain filter when it's not nightly.
        for (id, action) in self.transactions.iter() {
            let tx = transactions
                .get(id)
                .ok_or(AccordError::MissingTx(id.clone()))?;

            if tx.can_continue_execution(log, transactions)? {
                to_remove.push(id.clone());
                // TODO: Graceful executor error handling.
                match action {
                    BlockedAction::Read => {
                        let data = executor.read(&tx.proposed, &tx.inner).unwrap();
                        results.push(ExecutionActionOk::ReadOk(ReadOk {
                            tx: id.clone(),
                            data,
                        }));
                    }
                    BlockedAction::Apply(data) => {
                        log.write_applied(&tx.inner, &tx.proposed)?;
                        let data = executor.write(data, &tx.proposed, &tx.inner).unwrap();
                        results.push(ExecutionActionOk::ApplyOk(ApplyOk {
                            tx: id.clone(),
                            data,
                        }));
                    }
                };
            }
        }

        for id in to_remove.into_iter() {
            self.transactions.remove(&id);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accord::keys::ExactString;
    use crate::accord::timestamp::TimestampProvider;
    use crate::accord::{ReadData, WriteData};

    struct TestCoordinator {
        node: NodeId,
        ts_provider: TimestampProvider,
    }

    impl TestCoordinator {
        fn new() -> TestCoordinator {
            TestCoordinator {
                node: 1,
                ts_provider: TimestampProvider::new(1),
            }
        }

        fn new_tx(&self, kind: TransactionKind) -> Transaction<ExactString> {
            let ts = self.ts_provider.unique_now();
            let id = TransactionId(ts);
            let ks = KeySet::from_keys(vec![ExactString("test".to_string())]);
            let tx = Transaction::new(id, kind, ks, "command".to_string().into_bytes());
            tx
        }
    }

    struct EchoExecutor;

    impl Executor<ExactString> for EchoExecutor {
        type Error = ();

        fn read(
            &self,
            ts: &Timestamp,
            tx: &Transaction<ExactString>,
        ) -> Result<ReadData, Self::Error> {
            Ok(ReadData {
                data: tx.get_command().to_vec(),
            })
        }

        fn compute(
            &self,
            data: &ReadData,
            ts: &Timestamp,
            tx: &Transaction<ExactString>,
        ) -> Result<ComputeData, Self::Error> {
            Ok(ComputeData {
                data: tx.get_command().to_vec(),
            })
        }

        fn write(
            &self,
            data: &ComputeData,
            ts: &Timestamp,
            tx: &Transaction<ExactString>,
        ) -> Result<WriteData, Self::Error> {
            Ok(WriteData {
                data: tx.get_command().to_vec(),
            })
        }
    }

    #[test]
    fn simple_flow_no_deps() {
        logutil::init_test();

        let c = TestCoordinator::new();
        let tx = c.new_tx(TransactionKind::Read);
        let mut replica = ReplicaState::<ExactString>::new(c.node, Log::new());

        let preaccept = PreAccept { tx: tx.clone() };
        let ok = replica.receive_preaccept(preaccept);
        assert_eq!(tx.get_id(), &ok.tx);
        assert_eq!(tx.get_original_ts(), &ok.proposed);
        assert_eq!(0, ok.deps.len());

        let commit = Commit {
            tx: tx.clone(),
            timestamp: tx.get_original_ts().clone(),
            deps: Vec::new(),
        };
        replica.receive_commit(commit).unwrap();

        let read = Read {
            tx: tx.clone(),
            timestamp: tx.get_original_ts().clone(),
            deps: Vec::new(),
        };
        // Should immediately execute.
        let msgs = replica.receive_read(EchoExecutor, read).unwrap();
        assert_eq!(1, msgs.len());
        let expected_read = ExecutionActionOk::ReadOk(ReadOk {
            tx: tx.get_id().clone(),
            data: ReadData {
                data: tx.get_command().to_vec(),
            },
        });
        assert_eq!(expected_read, msgs[0]);

        let apply = Apply {
            tx: tx.clone(),
            timestamp: tx.get_original_ts().clone(),
            deps: Vec::new(),
            data: ComputeData { data: Vec::new() },
        };
        let msgs = replica.receive_apply(EchoExecutor, apply).unwrap();
        assert_eq!(1, msgs.len());
        let expected_apply = ExecutionActionOk::ApplyOk(ApplyOk {
            tx: tx.get_id().clone(),
            data: WriteData {
                data: tx.get_command().to_vec(),
            },
        });
        assert_eq!(expected_apply, msgs[0]);
    }

    #[test]
    fn simple_flow_conflict() {
        logutil::init_test();

        let c = TestCoordinator::new();

        let tx1 = c.new_tx(TransactionKind::Write);
        let tx2 = c.new_tx(TransactionKind::Write);
        let mut replica = ReplicaState::<ExactString>::new(c.node, Log::new());

        let preaccept1 = PreAccept { tx: tx1.clone() };
        let ok1 = replica.receive_preaccept(preaccept1);
        let ts1 = ok1.proposed;
        assert_eq!(ts1, tx1.get_original_ts().clone());

        let preaccept2 = PreAccept { tx: tx2.clone() };
        let ok2 = replica.receive_preaccept(preaccept2);
        let ts2 = ok2.proposed;
        let deps2 = ok2.deps;
        assert!(&ts2 > tx2.get_original_ts());
        assert_eq!(1, deps2.len());

        let accept1 = Accept {
            tx: tx1.clone(),
            timestamp: ts1.clone(),
            deps: Vec::new(),
        };
        let _ = replica.receive_accept(accept1);

        let accept2 = Accept {
            tx: tx2.clone(),
            timestamp: ts2.clone(),
            deps: deps2.clone(),
        };
        let _ = replica.receive_accept(accept2);

        // Try to commit and read tx1 before tx2. Commit should be successful,
        // but reading will be blocked until tx1 is committed and applied.

        let commit2 = Commit {
            tx: tx2.clone(),
            timestamp: ts2.clone(),
            deps: deps2.clone(),
        };
        replica.receive_commit(commit2).unwrap();

        let read2 = Read {
            tx: tx2.clone(),
            timestamp: ts2.clone(),
            deps: deps2.clone(),
        };
        let results = replica.receive_read(EchoExecutor, read2).unwrap();
        assert_eq!(0, results.len());

        // Commit and read for tx1.
        let commit1 = Commit {
            tx: tx1.clone(),
            timestamp: ts1.clone(),
            deps: Vec::new(),
        };
        replica.receive_commit(commit1).unwrap();

        let read1 = Read {
            tx: tx1.clone(),
            timestamp: ts1.clone(),
            deps: Vec::new(),
        };
        let results = replica.receive_read(EchoExecutor, read1).unwrap();
        assert_eq!(1, results.len());
        assert_eq!(tx1.get_id(), results[0].get_tx_id());

        let apply1 = Apply {
            tx: tx1.clone(),
            timestamp: ts1.clone(),
            deps: Vec::new(),
            data: ComputeData { data: Vec::new() },
        };
        let results = replica.receive_apply(EchoExecutor, apply1).unwrap();
        assert_eq!(2, results.len());

        // Transaction 2 can now finish applying.

        let apply2 = Apply {
            tx: tx2.clone(),
            timestamp: ts2.clone(),
            deps: deps2.clone(),
            data: ComputeData { data: Vec::new() },
        };
        let results = replica.receive_apply(EchoExecutor, apply2).unwrap();
        assert_eq!(1, results.len());
        assert_eq!(tx2.get_id(), results[0].get_tx_id());
    }
}
