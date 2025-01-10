//! Implementations of physical operators in an execution pipeline.

pub mod analyze;
pub mod copy_to;
pub mod create_schema;
pub mod create_table;
pub mod create_view;
pub mod drop;
pub mod empty;
pub mod filter;
pub mod hash_aggregate;
pub mod hash_join;
pub mod insert;
pub mod limit;
pub mod materialize;
pub mod nl_join;
pub mod project;
pub mod round_robin;
pub mod scan;
pub mod simple;
pub mod sink;
pub mod sort;
pub mod source;
pub mod table_function;
pub mod table_inout;
pub mod ungrouped_aggregate;
pub mod union;
pub mod unnest;
pub mod values;
pub mod window;

pub(crate) mod util;

#[cfg(test)]
mod testutil;

use std::fmt::Debug;
use std::sync::Arc;
use std::task::Context;

use copy_to::PhysicalCopyTo;
use create_schema::{CreateSchemaPartitionState, PhysicalCreateSchema};
use create_table::PhysicalCreateTable;
use create_view::{CreateViewPartitionState, PhysicalCreateView};
use drop::{DropPartitionState, PhysicalDrop};
use empty::PhysicalEmpty;
use filter::{FilterPartitionState, PhysicalFilter};
use hash_aggregate::PhysicalHashAggregate;
use hash_join::{
    HashJoinBuildPartitionState,
    HashJoinOperatorState,
    HashJoinProbePartitionState,
    PhysicalHashJoin,
};
use insert::PhysicalInsert;
use limit::PhysicalLimit;
use materialize::{MaterializeSourceOperation, MaterializedSinkOperation};
use nl_join::PhysicalNestedLoopJoin;
use project::{PhysicalProject, ProjectPartitionState};
use rayexec_error::{not_implemented, OptionExt, RayexecError, Result};
use round_robin::PhysicalRoundRobinRepartition;
use scan::{PhysicalScan, ScanPartitionState};
use sink::{SinkOperation, SinkOperator, SinkOperatorState, SinkPartitionState};
use sort::gather_sort::PhysicalGatherSort;
use sort::scatter_sort::PhysicalScatterSort;
use source::{SourceOperation, SourceOperator, SourcePartitionState};
use table_function::{PhysicalTableFunction, TableFunctionPartitionState};
use table_inout::{PhysicalTableInOut, TableInOutPartitionState};
use ungrouped_aggregate::{
    PhysicalUngroupedAggregate,
    UngroupedAggregateOperatorState,
    UngroupedAggregatePartitionState,
};
use union::{PhysicalUnion, UnionBottomPartitionState, UnionOperatorState, UnionTopPartitionState};
use unnest::{PhysicalUnnest, UnnestPartitionState};
use values::PhysicalValues;
use window::PhysicalWindow;

use self::empty::EmptyPartitionState;
use self::hash_aggregate::{HashAggregateOperatorState, HashAggregatePartitionState};
use self::limit::LimitPartitionState;
use self::nl_join::{
    NestedLoopJoinBuildPartitionState,
    NestedLoopJoinOperatorState,
    NestedLoopJoinProbePartitionState,
};
use self::round_robin::{
    RoundRobinOperatorState,
    RoundRobinPullPartitionState,
    RoundRobinPushPartitionState,
};
use self::simple::SimplePartitionState;
use self::sort::gather_sort::{
    GatherSortOperatorState,
    GatherSortPullPartitionState,
    GatherSortPushPartitionState,
};
use self::sort::scatter_sort::ScatterSortPartitionState;
use self::values::ValuesPartitionState;
use super::computed_batch::ComputedBatches;
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::engine::result::ResultSink;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

/// States local to a partition within a single operator.
// Current size: 264 bytes
#[derive(Debug)]
pub enum PartitionState {
    Project(ProjectPartitionState),
    Filter(FilterPartitionState),

    HashAggregate(HashAggregatePartitionState),
    UngroupedAggregate(UngroupedAggregatePartitionState),
    NestedLoopJoinBuild(NestedLoopJoinBuildPartitionState),
    NestedLoopJoinProbe(NestedLoopJoinProbePartitionState),
    HashJoinBuild(HashJoinBuildPartitionState),
    HashJoinProbe(HashJoinProbePartitionState),
    Values(ValuesPartitionState),
    Sink(SinkPartitionState),
    Source(SourcePartitionState),
    RoundRobinPush(RoundRobinPushPartitionState),
    RoundRobinPull(RoundRobinPullPartitionState),
    GatherSortPush(GatherSortPushPartitionState),
    GatherSortPull(GatherSortPullPartitionState),
    ScatterSort(ScatterSortPartitionState),
    Limit(LimitPartitionState),
    Unnest(UnnestPartitionState),
    UnionTop(UnionTopPartitionState),
    UnionBottom(UnionBottomPartitionState),
    Simple(SimplePartitionState),
    Scan(ScanPartitionState),
    TableFunction(TableFunctionPartitionState),
    TableInOut(TableInOutPartitionState),
    CreateSchema(CreateSchemaPartitionState),
    CreateView(CreateViewPartitionState),
    Drop(DropPartitionState),
    Empty(EmptyPartitionState),
    None,
}

/// A global state across all partitions in an operator.
// Current size: 144 bytes
#[derive(Debug)]
pub enum OperatorState {
    HashAggregate(HashAggregateOperatorState),
    UngroupedAggregate(UngroupedAggregateOperatorState),
    NestedLoopJoin(NestedLoopJoinOperatorState),
    HashJoin(HashJoinOperatorState),
    RoundRobin(RoundRobinOperatorState),
    GatherSort(GatherSortOperatorState),
    Union(UnionOperatorState),
    Sink(SinkOperatorState),
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
    /// Successfully received computed results.
    Computed(ComputedBatches),

    /// A batch could not be be retrieved right now.
    ///
    /// A waker will be registered for a later wakeup to try to pull the next
    /// batch.
    Pending,

    /// The operator has been exhausted for this partition.
    Exhausted,
}

/// Poll result for operator execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollExecute {
    /// Operator accepted input and wrote its output to the output batch.
    ///
    /// The next poll should be with a new input batch.
    Ready,
    /// Push pending. Waker stored, re-execute with the exact same state.
    Pending,
    /// Operator accepted as much input at can handle. Don't provide any
    /// additional input.
    Break,
    /// Operator needs more input before it'll produce any meaningful output.
    NeedsMore,
    /// Operator has more output. Call again with the same input batch.
    HasMore,
    /// No more output.
    Exhausted,
}

/// Poll result for operator finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollFinalize {
    /// Operator finalized, execution of this operator finished.
    ///
    /// `poll_execute` will not be called after this is returned.
    Finalized,
    /// This operator needs to be drained.
    ///
    /// `poll_execute` will be called with empty input batches until the
    /// opperator indicates it's been exhausted.
    NeedsDrain,
    /// Finalize pending, re-execute with the same state.
    Pending,
}

#[derive(Debug)]
pub struct ExecuteInOutState<'a> {
    /// Input batch being pushed to the operator.
    ///
    /// May be None for operators that are only producing output.
    input: Option<&'a mut Batch>,
    /// Output batch the operator should write to.
    ///
    /// May be None for operators that only consume batches.
    output: Option<&'a mut Batch>,
}

#[derive(Debug)]
pub enum PartitionAndOperatorStates {
    /// Operators that have a single input/output.
    Branchless {
        /// Global operator state.
        operator_state: OperatorState,
        /// State per-partition.
        partition_states: Vec<PartitionState>,
    },
    /// Operators that produce 1 or more output branches.
    ///
    /// Mostly for materializations.
    BranchingOutput {
        /// Global operator state.
        operator_state: OperatorState,
        /// Single set of input states.
        inputs_states: Vec<PartitionState>,
        /// Multiple sets of output states.
        output_states: Vec<Vec<PartitionState>>,
    },
    /// Operators that have two children, with this operator acting as the
    /// "sink" for one child.
    ///
    /// For joins, the build side is the terminating input, while the probe side
    /// is non-terminating.
    TerminatingInput {
        /// Global operator state.
        operator_state: OperatorState,
        /// States for the input that is non-terminating.
        nonterminating_states: Vec<PartitionState>,
        /// States for the input that is terminated by this operator.
        terminating_states: Vec<PartitionState>,
    },
}

impl PartitionAndOperatorStates {
    pub fn branchless_into_states(self) -> Result<(OperatorState, Vec<PartitionState>)> {
        match self {
            Self::Branchless {
                operator_state,
                partition_states,
            } => Ok((operator_state, partition_states)),
            Self::BranchingOutput { .. } => Err(RayexecError::new(
                "Expected branchless states, got branching output",
            )),
            Self::TerminatingInput { .. } => Err(RayexecError::new(
                "Expected branchless states, got terminating input",
            )),
        }
    }
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

pub trait ExecutableOperator: Sync + Send + Debug + Explainable {
    /// Create execution states for this operator.
    ///
    /// `input_partitions` is the partitioning for each input that will be
    /// pushing batches through this operator.
    ///
    /// Joins are assumed to have two inputs.
    fn create_states2(
        &self,
        _context: &DatabaseContext,
        _partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        unimplemented!()
    }

    fn create_states(
        &self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<PartitionAndOperatorStates> {
        unimplemented!()
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        unimplemented!()
    }

    /// Try to push a batch for this partition.
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    /// Finalize pushing to partition.
    ///
    /// This indicates the operator will receive no more input for a given
    /// partition, allowing the operator to execution some finalization logic.
    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }

    /// Try to pull a batch for this partition.
    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}

// 144 bytes
#[derive(Debug)]
pub enum PhysicalOperator {
    Project(PhysicalProject),
    Filter(PhysicalFilter),

    HashAggregate(PhysicalHashAggregate),
    UngroupedAggregate(PhysicalUngroupedAggregate),
    Window(PhysicalWindow),
    NestedLoopJoin(PhysicalNestedLoopJoin),
    HashJoin(PhysicalHashJoin),
    Values(PhysicalValues),
    ResultSink(SinkOperator<ResultSink>),
    DynSink(SinkOperator<Box<dyn SinkOperation>>),
    DynSource(SourceOperator<Box<dyn SourceOperation>>),
    MaterializedSink(SinkOperator<MaterializedSinkOperation>),
    MaterializedSource(SourceOperator<MaterializeSourceOperation>),
    RoundRobin(PhysicalRoundRobinRepartition),
    MergeSorted(PhysicalGatherSort),
    LocalSort(PhysicalScatterSort),
    Limit(PhysicalLimit),
    Union(PhysicalUnion),
    Unnest(PhysicalUnnest),
    Scan(PhysicalScan),
    TableFunction(PhysicalTableFunction),
    TableInOut(PhysicalTableInOut),
    Insert(PhysicalInsert),
    CopyTo(PhysicalCopyTo),
    CreateTable(PhysicalCreateTable),
    CreateSchema(PhysicalCreateSchema),
    CreateView(PhysicalCreateView),
    Drop(PhysicalDrop),
    Empty(PhysicalEmpty),
}

impl ExecutableOperator for PhysicalOperator {
    fn create_states2(
        &self,
        context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        match self {
            Self::HashAggregate(op) => op.create_states2(context, partitions),
            Self::UngroupedAggregate(op) => op.create_states2(context, partitions),
            Self::Window(op) => op.create_states2(context, partitions),
            Self::NestedLoopJoin(op) => op.create_states2(context, partitions),
            Self::HashJoin(op) => op.create_states2(context, partitions),
            Self::Values(op) => op.create_states2(context, partitions),
            Self::ResultSink(op) => op.create_states2(context, partitions),
            Self::DynSink(op) => op.create_states2(context, partitions),
            Self::DynSource(op) => op.create_states2(context, partitions),
            Self::MaterializedSink(op) => op.create_states2(context, partitions),
            Self::MaterializedSource(op) => op.create_states2(context, partitions),
            Self::RoundRobin(op) => op.create_states2(context, partitions),
            Self::MergeSorted(op) => op.create_states2(context, partitions),
            Self::LocalSort(op) => op.create_states2(context, partitions),
            Self::Limit(op) => op.create_states2(context, partitions),
            Self::Union(op) => op.create_states2(context, partitions),
            Self::Filter(op) => op.create_states2(context, partitions),
            Self::Project(op) => op.create_states2(context, partitions),
            Self::Unnest(op) => op.create_states2(context, partitions),
            Self::Scan(op) => op.create_states2(context, partitions),
            Self::TableFunction(op) => op.create_states2(context, partitions),
            Self::TableInOut(op) => op.create_states2(context, partitions),
            Self::Insert(op) => op.create_states2(context, partitions),
            Self::CopyTo(op) => op.create_states2(context, partitions),
            Self::CreateTable(op) => op.create_states2(context, partitions),
            Self::CreateSchema(op) => op.create_states2(context, partitions),
            Self::CreateView(op) => op.create_states2(context, partitions),
            Self::Drop(op) => op.create_states2(context, partitions),
            Self::Empty(op) => op.create_states2(context, partitions),
        }
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match self {
            Self::HashAggregate(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::UngroupedAggregate(op) => {
                op.poll_push(cx, partition_state, operator_state, batch)
            }
            Self::Window(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::NestedLoopJoin(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::HashJoin(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Values(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::ResultSink(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::DynSink(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::DynSource(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::MaterializedSink(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::MaterializedSource(op) => {
                op.poll_push(cx, partition_state, operator_state, batch)
            }
            Self::RoundRobin(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::MergeSorted(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::LocalSort(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Limit(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Union(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Filter(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Project(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Unnest(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Scan(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::TableFunction(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::TableInOut(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Insert(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::CopyTo(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::CreateTable(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::CreateSchema(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::CreateView(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Drop(op) => op.poll_push(cx, partition_state, operator_state, batch),
            Self::Empty(op) => op.poll_push(cx, partition_state, operator_state, batch),
        }
    }

    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        match self {
            Self::HashAggregate(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::UngroupedAggregate(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Window(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::NestedLoopJoin(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::HashJoin(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Values(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::ResultSink(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::DynSink(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::DynSource(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::MaterializedSink(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::MaterializedSource(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::RoundRobin(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::MergeSorted(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::LocalSort(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Limit(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Union(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Filter(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Project(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Unnest(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Scan(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::TableFunction(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::TableInOut(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Insert(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::CopyTo(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::CreateTable(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::CreateSchema(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::CreateView(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Drop(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Empty(op) => op.poll_finalize(cx, partition_state, operator_state),
        }
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match self {
            Self::HashAggregate(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::UngroupedAggregate(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Window(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::NestedLoopJoin(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::HashJoin(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Values(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::ResultSink(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::DynSink(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::DynSource(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::MaterializedSink(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::MaterializedSource(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::RoundRobin(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::MergeSorted(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::LocalSort(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Limit(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Union(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Filter(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Project(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Unnest(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Scan(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::TableFunction(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::TableInOut(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Insert(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::CopyTo(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::CreateTable(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::CreateSchema(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::CreateView(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Drop(op) => op.poll_pull(cx, partition_state, operator_state),
            Self::Empty(op) => op.poll_pull(cx, partition_state, operator_state),
        }
    }
}

impl Explainable for PhysicalOperator {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        match self {
            Self::HashAggregate(op) => op.explain_entry(conf),
            Self::UngroupedAggregate(op) => op.explain_entry(conf),
            Self::Window(op) => op.explain_entry(conf),
            Self::NestedLoopJoin(op) => op.explain_entry(conf),
            Self::HashJoin(op) => op.explain_entry(conf),
            Self::Values(op) => op.explain_entry(conf),
            Self::ResultSink(op) => op.explain_entry(conf),
            Self::DynSink(op) => op.explain_entry(conf),
            Self::DynSource(op) => op.explain_entry(conf),
            Self::MaterializedSink(op) => op.explain_entry(conf),
            Self::MaterializedSource(op) => op.explain_entry(conf),
            Self::RoundRobin(op) => op.explain_entry(conf),
            Self::MergeSorted(op) => op.explain_entry(conf),
            Self::LocalSort(op) => op.explain_entry(conf),
            Self::Limit(op) => op.explain_entry(conf),
            Self::Union(op) => op.explain_entry(conf),
            Self::Filter(op) => op.explain_entry(conf),
            Self::Project(op) => op.explain_entry(conf),
            Self::Unnest(op) => op.explain_entry(conf),
            Self::Scan(op) => op.explain_entry(conf),
            Self::TableFunction(op) => op.explain_entry(conf),
            Self::TableInOut(op) => op.explain_entry(conf),
            Self::Insert(op) => op.explain_entry(conf),
            Self::CopyTo(op) => op.explain_entry(conf),
            Self::CreateTable(op) => op.explain_entry(conf),
            Self::CreateSchema(op) => op.explain_entry(conf),
            Self::CreateView(op) => op.explain_entry(conf),
            Self::Drop(op) => op.explain_entry(conf),
            Self::Empty(op) => op.explain_entry(conf),
        }
    }
}

impl DatabaseProtoConv for PhysicalOperator {
    type ProtoType = rayexec_proto::generated::execution::PhysicalOperator;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::execution::physical_operator::Value;

        let value = match self {
            Self::CreateSchema(op) => Value::CreateSchema(op.to_proto_ctx(context)?),
            Self::CreateTable(op) => Value::CreateTable(op.to_proto_ctx(context)?),
            Self::Drop(op) => Value::Drop(op.to_proto_ctx(context)?),
            Self::Empty(op) => Value::Empty(op.to_proto_ctx(context)?),
            // Self::Filter(op) => Value::Filter(op.to_proto_ctx(context)?),
            // Self::Project(op) => Value::Project(op.to_proto_ctx(context)?),
            Self::Insert(op) => Value::Insert(op.to_proto_ctx(context)?),
            Self::Limit(op) => Value::Limit(op.to_proto_ctx(context)?),
            Self::Scan(op) => Value::Scan(op.to_proto_ctx(context)?),
            Self::UngroupedAggregate(op) => Value::UngroupedAggregate(op.to_proto_ctx(context)?),
            Self::Union(op) => Value::Union(op.to_proto_ctx(context)?),
            Self::Values(op) => Value::Values(op.to_proto_ctx(context)?),
            Self::TableFunction(op) => Value::TableFunction(op.to_proto_ctx(context)?),
            Self::NestedLoopJoin(op) => Value::NlJoin(op.to_proto_ctx(context)?),
            Self::CopyTo(op) => Value::CopyTo(op.to_proto_ctx(context)?),
            Self::LocalSort(op) => Value::LocalSort(op.to_proto_ctx(context)?),
            Self::MergeSorted(op) => Value::MergeSorted(op.to_proto_ctx(context)?),
            other => not_implemented!("to proto: {other:?}"),
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        use rayexec_proto::generated::execution::physical_operator::Value;

        Ok(match proto.value.required("value")? {
            Value::CreateSchema(op) => {
                PhysicalOperator::CreateSchema(PhysicalCreateSchema::from_proto_ctx(op, context)?)
            }
            Value::CreateTable(op) => {
                PhysicalOperator::CreateTable(PhysicalCreateTable::from_proto_ctx(op, context)?)
            }
            Value::Drop(op) => PhysicalOperator::Drop(PhysicalDrop::from_proto_ctx(op, context)?),
            Value::Empty(op) => {
                PhysicalOperator::Empty(PhysicalEmpty::from_proto_ctx(op, context)?)
            }
            // Value::Filter(op) => {
            //     PhysicalOperator::Filter(PhysicalFilter2::from_proto_ctx(op, context)?)
            // }
            // Value::Project(op) => {
            //     PhysicalOperator::Project(PhysicalProject2::from_proto_ctx(op, context)?)
            // }
            Value::Insert(op) => {
                PhysicalOperator::Insert(PhysicalInsert::from_proto_ctx(op, context)?)
            }
            Value::Limit(op) => {
                PhysicalOperator::Limit(PhysicalLimit::from_proto_ctx(op, context)?)
            }
            Value::Scan(op) => PhysicalOperator::Scan(PhysicalScan::from_proto_ctx(op, context)?),
            Value::UngroupedAggregate(op) => PhysicalOperator::UngroupedAggregate(
                PhysicalUngroupedAggregate::from_proto_ctx(op, context)?,
            ),
            Value::Union(op) => {
                PhysicalOperator::Union(PhysicalUnion::from_proto_ctx(op, context)?)
            }
            Value::Values(op) => {
                PhysicalOperator::Values(PhysicalValues::from_proto_ctx(op, context)?)
            }
            Value::TableFunction(op) => {
                PhysicalOperator::TableFunction(PhysicalTableFunction::from_proto_ctx(op, context)?)
            }
            Value::NlJoin(op) => PhysicalOperator::NestedLoopJoin(
                PhysicalNestedLoopJoin::from_proto_ctx(op, context)?,
            ),
            Value::CopyTo(op) => {
                PhysicalOperator::CopyTo(PhysicalCopyTo::from_proto_ctx(op, context)?)
            }
            Value::LocalSort(op) => {
                PhysicalOperator::LocalSort(PhysicalScatterSort::from_proto_ctx(op, context)?)
            }
            Value::MergeSorted(op) => {
                PhysicalOperator::MergeSorted(PhysicalGatherSort::from_proto_ctx(op, context)?)
            }
            _ => unimplemented!(),
        })
    }
}
