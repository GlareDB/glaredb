//! Implementations of physical operators in an execution pipeline.

pub mod copy_to;
pub mod create_schema;
pub mod create_table;
pub mod drop;
pub mod empty;
pub mod filter;
pub mod hash_aggregate;
pub mod insert;
pub mod ipc;
pub mod join;
pub mod limit;
pub mod materialize;
pub mod project;
pub mod round_robin;
pub mod scan;
pub mod simple;
pub mod sink;
pub mod sort;
pub mod table_function;
pub mod ungrouped_aggregate;
pub mod union;
pub mod values;

mod util;

#[cfg(test)]
mod test_util;

use copy_to::CopyToPartitionState;
use create_schema::CreateSchemaPartitionState;
use create_table::{CreateTableOperatorState, CreateTablePartitionState};
use drop::DropPartitionState;
use insert::InsertPartitionState;
use materialize::{
    MaterializeOperatorState, MaterializePullPartitionState, MaterializePushPartitionState,
};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use scan::ScanPartitionState;
use sink::QuerySinkPartitionState;
use std::fmt::Debug;
use std::sync::Arc;
use std::task::Context;
use table_function::TableFunctionPartitionState;
use ungrouped_aggregate::{UngroupedAggregateOperatorState, UngroupedAggregatePartitionState};
use union::{UnionBottomPartitionState, UnionOperatorState, UnionTopPartitionState};

use crate::database::DatabaseContext;
use crate::logical::explainable::Explainable;

use self::empty::EmptyPartitionState;
use self::hash_aggregate::{HashAggregateOperatorState, HashAggregatePartitionState};
use self::join::hash_join::{
    HashJoinBuildPartitionState, HashJoinOperatorState, HashJoinProbePartitionState,
};
use self::join::nl_join::{
    NestedLoopJoinBuildPartitionState, NestedLoopJoinOperatorState,
    NestedLoopJoinProbePartitionState,
};
use self::limit::LimitPartitionState;
use self::round_robin::{
    RoundRobinOperatorState, RoundRobinPullPartitionState, RoundRobinPushPartitionState,
};
use self::simple::SimplePartitionState;
use self::sort::local_sort::LocalSortPartitionState;
use self::sort::merge_sorted::{
    MergeSortedOperatorState, MergeSortedPullPartitionState, MergeSortedPushPartitionState,
};
use self::values::ValuesPartitionState;

/// States local to a partition within a single operator.
// Current size: 192 bytes
#[derive(Debug)]
pub enum PartitionState {
    HashAggregate(HashAggregatePartitionState),
    UngroupedAggregate(UngroupedAggregatePartitionState),
    NestedLoopJoinBuild(NestedLoopJoinBuildPartitionState),
    NestedLoopJoinProbe(NestedLoopJoinProbePartitionState),
    HashJoinBuild(HashJoinBuildPartitionState),
    HashJoinProbe(HashJoinProbePartitionState),
    Values(ValuesPartitionState),
    QuerySink(QuerySinkPartitionState),
    RoundRobinPush(RoundRobinPushPartitionState),
    RoundRobinPull(RoundRobinPullPartitionState),
    MergeSortedPush(MergeSortedPushPartitionState),
    MergeSortedPull(MergeSortedPullPartitionState),
    LocalSort(LocalSortPartitionState),
    Limit(LimitPartitionState),
    MaterializePush(MaterializePushPartitionState),
    MaterializePull(MaterializePullPartitionState),
    UnionTop(UnionTopPartitionState),
    UnionBottom(UnionBottomPartitionState),
    Simple(SimplePartitionState),
    Scan(ScanPartitionState),
    TableFunction(TableFunctionPartitionState),
    Insert(InsertPartitionState),
    CopyTo(CopyToPartitionState),
    CreateTable(CreateTablePartitionState),
    CreateSchema(CreateSchemaPartitionState),
    Drop(DropPartitionState),
    Empty(EmptyPartitionState),
    None,
}

/// A global state across all partitions in an operator.
// Current size: 112 bytes
#[derive(Debug)]
pub enum OperatorState {
    HashAggregate(HashAggregateOperatorState),
    UngroupedAggregate(UngroupedAggregateOperatorState),
    NestedLoopJoin(NestedLoopJoinOperatorState),
    HashJoin(HashJoinOperatorState),
    RoundRobin(RoundRobinOperatorState),
    MergeSorted(MergeSortedOperatorState),
    Materialize(MaterializeOperatorState),
    Union(UnionOperatorState),
    CreateTable(CreateTableOperatorState),
    None,
}

/// Result of a push to an operator.
///
/// An operator may not be ready to accept input either because it's waiting on
/// something else to complete (e.g. the right side of a join needs to the left
/// side to complete first) or some internal buffer is full.
#[derive(Debug, PartialEq)]
pub enum PollPush {
    /// Batch was successfully pushed.
    Pushed,

    /// Batch could not be processed right now.
    ///
    /// A waker will be registered for a later wakeup. This same batch should be
    /// pushed at that time.
    Pending(Batch),

    /// This operator requires no more input.
    ///
    /// `finalize_push` for the operator should _not_ be called.
    Break,

    /// Batch was successfully pushed, but the operator needs more input before
    /// it can start producing output
    NeedsMore,
}

/// Result of a pull from a Source.
#[derive(Debug, PartialEq)]
pub enum PollPull {
    /// Successfully received a data batch.
    Batch(Batch),

    /// A batch could not be be retrieved right now.
    ///
    /// A waker will be registered for a later wakeup to try to pull the next
    /// batch.
    Pending,

    /// The operator has been exhausted for this partition.
    Exhausted,
}

#[derive(Debug, PartialEq)]
pub enum PollFinalize {
    Finalized,
    Pending,
}

/// Describes the relationships of partition states for operators.
#[derive(Debug)]
pub enum InputOutputStates {
    /// Input and output partition states have a one-to-one mapping.
    ///
    /// The states used for pushing to an operator are the same states used to
    /// pull from the operator.
    ///
    /// This variant should also be used for pure source and pure sink operators
    /// where states are only ever used for pushing or pulling.
    OneToOne {
        /// Per-partition operators states.
        ///
        /// Length of vec determines the partitioning (parallelism) of the
        /// operator.
        partition_states: Vec<PartitionState>,
    },

    /// Operators accepts multiple inputs, and a single output.
    ///
    /// A single set of input states are used during pull.
    NaryInputSingleOutput {
        /// Per-input, per-partition operators states.
        ///
        /// The outer vec matches the number of inputs to an operator (e.g. a
        /// join should have two).
        partition_states: Vec<Vec<PartitionState>>,

        /// Index into the above vec to determine which set of states are used
        /// for pulling.
        ///
        /// For joins, the partition states for probes are the ones used for
        /// pulling.
        ///
        /// The chosen set of states indicates the output partitioning for the
        /// operator.
        pull_states: usize,
    },

    /// Partition states for an operator that accepts a single input, and
    /// produce 'n' outputs.
    SingleInputNaryOutput {
        /// States for the single input during push.
        push_states: Vec<PartitionState>,

        /// States for the n outputs.
        pull_states: Vec<Vec<PartitionState>>,
    },

    /// Partition states between the push side and pull side are separate.
    ///
    /// This provides a way for operators to output a different number of
    /// partitions than it receives.
    ///
    /// Operators that need this will introduce a pipeline split where the push
    /// states are used for pipeline's sink, while the pull states are used for
    /// the source of a separate pipeline.
    SeparateInputOutput {
        /// States used during push.
        push_states: Vec<PartitionState>,

        /// States used during pull.
        pull_states: Vec<PartitionState>,
    },
}

/// States generates from an operator to use during execution.
#[derive(Debug)]
pub struct ExecutionStates {
    /// Global operator state.
    pub operator_state: Arc<OperatorState>,

    /// Partition states for the operator.
    pub partition_states: InputOutputStates,
}

pub trait PhysicalOperator: Sync + Send + Debug + Explainable {
    /// Create execution states for this operator.
    ///
    /// `input_partitions` is the partitioning for each input that will be
    /// pushing batches through this operator.
    ///
    /// Joins are assumed to have two inputs.
    fn create_states(
        &self,
        _context: &DatabaseContext,
        _partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        unimplemented!("{self:?}")
    }

    /// Try to push a batch for this partition.
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush>;

    /// Finalize pushing to partition.
    ///
    /// This indicates the operator will receive no more input for a given
    /// partition, allowing the operator to execution some finalization logic.
    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize>;

    /// Try to pull a batch for this partition.
    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull>;
}
