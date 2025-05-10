mod hash_table;

use std::task::Context;

use glaredb_error::Result;
use hash_table::scan::HashTablePartitionScanState;
use hash_table::{
    HashJoinCondition,
    HashTableBuildPartitionState,
    HashTableOperatorState,
    JoinHashTable,
};
use parking_lot::Mutex;

use super::util::delayed_count::DelayedPartitionCount;
use super::util::partition_wakers::PartitionWakers;
use super::{
    BaseOperator,
    ExecuteOperator,
    ExecutionProperties,
    PollExecute,
    PollFinalize,
    PollPush,
    PushOperator,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::logical::logical_join::JoinType;

#[derive(Debug)]
pub struct HashJoinOperatorState {
    /// Global table we're inserting into.
    table: JoinHashTable,
    /// Operator state for the table.
    table_state: HashTableOperatorState,
    shared: Mutex<SharedState>,
}

#[derive(Debug)]
struct SharedState {
    /// If we're ready to begin inserting hashes.
    hash_inserts_ready: bool,
    /// If we've collected everything from the build side and have the directory
    /// initialized.
    scan_ready: bool,
    /// Number of partitions still inserting hashes.
    ///
    /// Once zero, we can begin scanning.
    remaining_hash_inserters: DelayedPartitionCount,
    /// Partition wakers for build-side partitions that have completed inserting
    /// into the table, and are waiting for the directory to be initialized to
    /// begin inserting hashes.
    pending_hash_inserters: PartitionWakers,
    /// Partition wakers for the probe side if the scan isn't ready.
    pending_probers: PartitionWakers,
}

#[derive(Debug)]
pub struct HashJoinPartitionBuildState {
    /// Bool indicating if we're in the hash inserting phase of finalizing.
    finalizing_hash_insert: bool,
    /// State used for inserting into the hash table.
    build_state: HashTableBuildPartitionState,
}

#[derive(Debug)]
pub struct HashJoinPartitionProbeState {
    /// If we're actually ready for scanning.
    ///
    /// If false, must check shared state before continuing.
    scan_ready: bool,
    /// Scan state for the hash table.
    scan_state: HashTablePartitionScanState,
}

#[derive(Debug)]
pub struct PhysicalHashJoin {
    /// Join type.
    pub(crate) join_type: JoinType,
    /// Types from the left side of the join.
    pub(crate) left_types: Vec<DataType>,
    /// Types from the right side of the join.
    pub(crate) right_types: Vec<DataType>,
    /// Output types for the join, dependent on the join type.
    pub(crate) output_types: Vec<DataType>,
    /// Join conditions.
    pub(crate) conditions: Vec<HashJoinCondition>,
}

impl PhysicalHashJoin {
    pub fn new(
        join_type: JoinType,
        left_types: impl IntoIterator<Item = DataType>,
        right_types: impl IntoIterator<Item = DataType>,
        conditions: impl IntoIterator<Item = HashJoinCondition>,
    ) -> Result<Self> {
        let left_types: Vec<_> = left_types.into_iter().collect();
        let right_types: Vec<_> = right_types.into_iter().collect();

        let output_types = match join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => left_types.clone(),
            JoinType::Right | JoinType::Full | JoinType::Left | JoinType::Inner => left_types
                .iter()
                .cloned()
                .chain(right_types.iter().cloned())
                .collect(),
            JoinType::LeftMark { .. } => {
                let mut types = left_types.clone();
                types.push(DataType::boolean());
                types
            }
        };

        Ok(PhysicalHashJoin {
            join_type,
            left_types,
            right_types,
            output_types,
            conditions: conditions.into_iter().collect(),
        })
    }
}

impl BaseOperator for PhysicalHashJoin {
    const OPERATOR_NAME: &str = "HashJoin";

    type OperatorState = HashJoinOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        let table = JoinHashTable::try_new(
            self.join_type,
            self.left_types.clone(),
            self.right_types.clone(),
            self.conditions.clone(),
            props.batch_size,
        )?;

        let table_state = table.create_operator_state()?;

        Ok(HashJoinOperatorState {
            table,
            table_state,
            shared: Mutex::new(SharedState {
                hash_inserts_ready: false,
                scan_ready: false,
                remaining_hash_inserters: DelayedPartitionCount::uninit(),
                pending_hash_inserters: PartitionWakers::empty(),
                pending_probers: PartitionWakers::empty(),
            }),
        })
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }
}

impl PushOperator for PhysicalHashJoin {
    type PartitionPushState = HashJoinPartitionBuildState;

    fn create_partition_push_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>> {
        let table = &operator_state.table;
        let table_state = &operator_state.table_state;

        let states = table.create_build_partition_states(table_state, partitions)?;
        let states = states
            .into_iter()
            .map(|state| HashJoinPartitionBuildState {
                finalizing_hash_insert: false,
                build_state: state,
            })
            .collect();

        Ok(states)
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        let table = &operator_state.table;
        let table_state = &operator_state.table_state;

        table.collect_build(table_state, &mut state.build_state, input)?;

        Ok(PollPush::NeedsMore)
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize> {
        let table = &operator_state.table;
        let table_state = &operator_state.table_state;

        loop {
            if state.finalizing_hash_insert {
                // We're in the hash inserting phase.
                let mut shared = operator_state.shared.lock();
                if !shared.hash_inserts_ready {
                    // We're not ready to insert the hashes (directory not
                    // ready). Come back later.
                    shared
                        .pending_hash_inserters
                        .store(cx.waker(), state.build_state.partition_idx);
                    return Ok(PollFinalize::Pending);
                }
                std::mem::drop(shared);

                // SAFETY: We've indicated that hash inserts are ready, so we should
                // have the directory available.
                //
                // Parallel inserts from multiple partitions is ok.
                unsafe { table.process_hashes(table_state, &mut state.build_state)? }

                let mut shared = operator_state.shared.lock();
                let remaining = shared.remaining_hash_inserters.dec_by_one()?;
                if remaining == 0 {
                    // We're the last partition to complete inserting hashes.
                    // Wake up all pending probers.
                    shared.scan_ready = true;
                    shared.pending_probers.wake_all();
                }

                return Ok(PollFinalize::Finalized);
            } else {
                // We're finalizing our build.
                state.finalizing_hash_insert = true;
                let is_last = table.finish_build(table_state, &mut state.build_state)?;

                if is_last {
                    // We're the last partition. Init the directory.
                    unsafe { table.init_directory(table_state)? };

                    // Now wake up all pending hash inserters.
                    let mut shared = operator_state.shared.lock();
                    shared.hash_inserts_ready = true;
                    shared.pending_hash_inserters.wake_all();

                    // Jump directly to inserting.
                    continue;
                } else {
                    // Other partitions still building, we'll need to wait until we
                    // can insert the hashes.
                    let mut shared = operator_state.shared.lock();
                    shared
                        .pending_hash_inserters
                        .store(cx.waker(), state.build_state.partition_idx);

                    return Ok(PollFinalize::Pending);
                }
            }
        }
    }
}

impl ExecuteOperator for PhysicalHashJoin {
    type PartitionExecuteState = HashJoinPartitionProbeState;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        let table = &operator_state.table;
        let table_state = &operator_state.table_state;

        let states = table.create_probe_partition_states(table_state, partitions)?;
        let states = states
            .into_iter()
            .map(|state| HashJoinPartitionProbeState {
                scan_ready: false,
                scan_state: state,
            })
            .collect();

        Ok(states)
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        _state: &mut Self::PartitionExecuteState,
        _input: &mut Batch,
        _output: &mut Batch,
    ) -> Result<PollExecute> {
        unimplemented!()
    }

    fn poll_finalize_execute(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        _state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }
}

impl Explainable for PhysicalHashJoin {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new(Self::OPERATOR_NAME, conf).build()
    }
}
