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
pub mod join;
pub mod limit;
pub mod materialize;
pub mod project;
pub mod results;
pub mod sink;
pub mod sort;
pub mod source;
pub mod table_inout;
pub mod ungrouped_aggregate;
pub mod union;
pub mod unnest;
pub mod values;
pub mod window;

pub(crate) mod util;

use std::fmt::Debug;
use std::task::Context;

use copy_to::PhysicalCopyTo;
use create_schema::{CreateSchemaPartitionState, PhysicalCreateSchema};
use create_table::PhysicalCreateTable;
use create_view::{CreateViewPartitionState, PhysicalCreateView};
use drop::{DropPartitionState, PhysicalDrop};
use empty::PhysicalEmpty;
use filter::{FilterPartitionState, PhysicalFilter};
use hash_aggregate::PhysicalHashAggregate;
use insert::PhysicalInsert;
use join::nested_loop_join::{
    NestedLoopJoinBuildPartitionState,
    NestedLoopJoinOperatorState,
    NestedLoopJoinProbePartitionState,
    PhysicalNestedLoopJoin,
};
use limit::PhysicalLimit;
use project::{PhysicalProject, ProjectPartitionState};
use rayexec_error::{not_implemented, OptionExt, RayexecError, Result};
use sink::operation::SinkOperation;
use sink::{PhysicalSink, SinkOperatorState, SinkPartitionState};
use sort::{SortOperatorState, SortPartitionState};
use source::table_function::{PhysicalTableFunction, TableFunctionPartitionState};
use table_inout::{PhysicalTableInOut, TableInOutPartitionState};
use ungrouped_aggregate::{
    PhysicalUngroupedAggregate,
    UngroupedAggregateOperatorState,
    UngroupedAggregatePartitionState,
};
use union::{
    PhysicalUnion,
    UnionBufferingPartitionState,
    UnionOperatorState,
    UnionPullingPartitionState,
};
use unnest::{PhysicalUnnest, UnnestPartitionState};
use values::PhysicalValues;
use window::PhysicalWindow;

use self::hash_aggregate::{HashAggregateOperatorState, HashAggregatePartitionState};
use self::limit::LimitPartitionState;
use self::values::ValuesPartitionState;
use super::pipeline::{ExecutablePipeline, ExecutablePipelineGraph};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;
use crate::engine::result::ResultSink;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;
use crate::ptr::raw_clone_ptr::RawClonePtr;
use crate::ptr::raw_ptr::RawPtr;

// TODO: Remove in favor of passing in batch size when creating states.
pub const DEFAULT_TARGET_BATCH_SIZE: usize = 4096;

/// States local to a partition within a single operator.
// Current size: 264 bytes
#[derive(Debug)]
pub enum PartitionState {
    Limit(LimitPartitionState),
    Project(ProjectPartitionState),
    Filter(FilterPartitionState),
    UnionPulling(UnionPullingPartitionState),
    UnionBuffering(UnionBufferingPartitionState),
    NestedLoopJoinBuild(NestedLoopJoinBuildPartitionState),
    NestedLoopJoinProbe(NestedLoopJoinProbePartitionState),
    Sort(SortPartitionState),
    UngroupedAggregate(UngroupedAggregatePartitionState),
    HashAggregate(HashAggregatePartitionState),
    TableFunction(TableFunctionPartitionState),

    // HashJoinBuild(HashJoinBuildPartitionState),
    // HashJoinProbe(HashJoinProbePartitionState),
    Values(ValuesPartitionState),
    Sink(SinkPartitionState),
    // GatherSortPush(GatherSortPushPartitionState),
    // GatherSortPull(GatherSortPullPartitionState),
    // ScatterSort(ScatterSortPartitionState),
    Unnest(UnnestPartitionState),
    TableInOut(TableInOutPartitionState),
    CreateSchema(CreateSchemaPartitionState),
    CreateView(CreateViewPartitionState),
    Drop(DropPartitionState),
    None,
}

/// A global state across all partitions in an operator.
// Current size: 144 bytes
#[derive(Debug)]
pub enum OperatorState {
    Union(UnionOperatorState),
    NestedLoopJoin(NestedLoopJoinOperatorState),
    Sort(SortOperatorState),
    UngroupedAggregate(UngroupedAggregateOperatorState),
    HashAggregate(HashAggregateOperatorState),

    // HashJoin(HashJoinOperatorState),
    // GatherSort(GatherSortOperatorState),
    Sink(SinkOperatorState),
    None,
}

/// Poll result for operator execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollExecute {
    /// Operator accepted input and wrote its output to the output batch.
    ///
    /// The next poll should be with a new input batch.
    Ready,
    /// Execution pending. Waker stored, re-execute with the exact same state.
    Pending,
    /// Operator needs more input before it'll produce any meaningful output.
    NeedsMore,
    /// Operator has more output. Call again with the same input batch.
    ///
    /// Meaningful output was written to the output batch and should be pushed
    /// to next operator(s).
    HasMore,
    /// Operator is exhausted and shouldn't be polled again.
    ///
    /// The output batch will have any remaining data. If there's no more data,
    /// then the output batch will have zero rows.
    ///
    /// When we're executing the pipeline, an operator returning this will
    /// prevent this operator and any child operators within that pipeline from
    /// being executed again.
    ///
    /// E.g. the LIMIT operator will return Exhausted once the limit has been
    /// reached.
    Exhausted,
}

/// Output of a pull operator.
///
/// Subset of `PollExecute`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollPull {
    Pending,
    HasMore,
    Exhausted,
}

impl PollPull {
    pub const fn as_poll_execute(self) -> PollExecute {
        match self {
            Self::Pending => PollExecute::Pending,
            Self::HasMore => PollExecute::HasMore,
            Self::Exhausted => PollExecute::Exhausted,
        }
    }
}

/// Output of a push operator.
///
/// Subset of `PollExecute`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollPush {
    Pending,
    NeedsMore,
}

impl PollPush {
    pub const fn as_poll_execute(self) -> PollExecute {
        match self {
            Self::Pending => PollExecute::Pending,
            Self::NeedsMore => PollExecute::NeedsMore,
        }
    }
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
pub struct ExecuteInOut<'a> {
    /// Input batch being pushed to the operator.
    ///
    /// May be None for operators that are only producing output.
    pub input: Option<&'a mut Batch>,
    /// Output batch the operator should write to.
    ///
    /// May be None for operators that only consume batches.
    pub output: Option<&'a mut Batch>,
}

#[derive(Debug)]
pub enum PartitionAndOperatorStates {
    /// Operator that accepts a single input and produces a single output.
    UnaryInput(UnaryInputStates),
    /// Operator that accepts two inputs and produces a single output.
    BinaryInput(BinaryInputStates),
    /// Operator that materializes its input and produces multiple outputs.
    Materialization(MaterializationStates),
}

impl From<UnaryInputStates> for PartitionAndOperatorStates {
    fn from(states: UnaryInputStates) -> Self {
        PartitionAndOperatorStates::UnaryInput(states)
    }
}

impl From<BinaryInputStates> for PartitionAndOperatorStates {
    fn from(states: BinaryInputStates) -> Self {
        PartitionAndOperatorStates::BinaryInput(states)
    }
}

impl From<MaterializationStates> for PartitionAndOperatorStates {
    fn from(states: MaterializationStates) -> Self {
        PartitionAndOperatorStates::Materialization(states)
    }
}

#[derive(Debug)]
pub struct UnaryInputStates {
    /// Global operator state.
    pub operator_state: OperatorState,
    /// State per-partition.
    pub partition_states: Vec<PartitionState>,
}

#[derive(Debug)]
pub struct BinaryInputStates {
    /// Global operator state.
    pub operator_state: OperatorState,
    /// States that belong to the "sink" input for the operator.
    ///
    /// E.g. states for the build side of join.
    pub sink_states: Vec<PartitionState>,
    /// States for the input that "passes through" the operator in the sense
    /// that this input is treats the operator as a normal intermediate operator
    /// in the chain.
    ///
    /// E.g. states for the probe side of a join.
    pub inout_states: Vec<PartitionState>,
}

#[derive(Debug)]
pub struct MaterializationStates {
    /// Global operator state.
    pub operator_state: OperatorState,
    /// States for the single input into the materialization operator.
    pub input_states: Vec<PartitionState>,
    /// States for the outputs from the materialization operator.
    pub output_states: Vec<Vec<PartitionState>>,
}

pub trait ExecutableOperator: Sync + Send + Debug + Explainable {
    type States: Into<PartitionAndOperatorStates>;

    fn output_types(&self) -> &[DataType] {
        unimplemented!()
    }

    fn create_states(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Self::States> {
        unimplemented!()
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOut,
    ) -> Result<PollExecute> {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionProperties {
    /// Batch size, in rows, that were working with.
    pub batch_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorType {
    Execute,
    Pull,
    Push,
    PushExecute,
}

impl OperatorType {
    pub const fn children_requirement(&self) -> usize {
        match self {
            Self::Execute | Self::Push => 1,
            Self::Pull => 0,
            Self::PushExecute => 2,
        }
    }
}

pub trait BaseOperator: Sync + Send + Debug + Explainable {
    type OperatorState: Sync + Send;

    fn create_operator_state(
        &self,
        context: &DatabaseContext,
        props: ExecutionProperties,
    ) -> Result<Self::OperatorState>;

    fn output_types(&self) -> &[DataType];

    fn build_pipeline(
        operator: PlannedOperatorWithChildren,
        db_context: &DatabaseContext,
        props: ExecutionProperties,
        graph: &mut ExecutablePipelineGraph,
        current: &mut ExecutablePipeline,
    ) -> Result<()> {
        let children = &operator.children;
        if children.len() != operator.as_ref().operator_type.children_requirement() {
            return Err(RayexecError::new("Unexpected number of children"));
        }

        match operator.as_ref().operator_type {
            OperatorType::Execute | OperatorType::Push => {
                // Plan child operator.
                children[0]
                    .as_ref()
                    .call_build_pipeline(db_context, props, graph, current)?;

                // Push self onto pipeline.
                let state = operator
                    .as_ref()
                    .call_create_operator_state(db_context, props)?;
                current.push_operator_and_state(operator.operator, state);

                Ok(())
            }
            OperatorType::Pull => {
                // No children, just push self.
                let state = operator
                    .as_ref()
                    .call_create_operator_state(db_context, props)?;
                current.push_operator_and_state(operator.operator, state);

                Ok(())
            }
            OperatorType::PushExecute => {
                // Create new pipeline for left/push child.
                let mut left_pipeline = ExecutablePipeline::new();
                children[0].as_ref().call_build_pipeline(
                    db_context,
                    props,
                    graph,
                    &mut left_pipeline,
                )?;

                // Now build up the right/execute child using the current
                // pipeline.
                children[0]
                    .as_ref()
                    .call_build_pipeline(db_context, props, graph, current)?;

                // Push operator and state to both pipelines.
                let state = operator
                    .as_ref()
                    .call_create_operator_state(db_context, props)?;
                left_pipeline.push_operator_and_state(operator.as_ref().clone(), state.clone());
                current.push_operator_and_state(operator.as_ref().clone(), state.clone());

                // Left pipeline finished, push to query graph.
                graph.push_pipeline(left_pipeline);

                Ok(())
            }
        }
    }
}

pub trait ExecuteOperator: BaseOperator {
    type PartitionExecuteState: Sync + Send;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>>;

    fn poll_execute(
        &self,
        cx: &mut Context,
        state: &mut Self::PartitionExecuteState,
        operator_state: &Self::OperatorState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute>;

    fn poll_finalize_execute(
        &self,
        cx: &mut Context,
        state: &mut Self::PartitionExecuteState,
        operator_state: &Self::OperatorState,
    ) -> Result<PollFinalize>;
}

pub trait PullOperator: BaseOperator {
    type PartitionPullState: Sync + Send;

    fn create_partition_pull_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPullState>>;

    fn poll_pull(
        &self,
        cx: &mut Context,
        state: &mut Self::PartitionPullState,
        operator_state: &Self::OperatorState,
        output: &mut Batch,
    ) -> Result<PollPull>;
}

pub trait PushOperator: BaseOperator {
    type PartitionPushState: Sync + Send;

    fn create_partition_push_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>>;

    fn poll_push(
        &self,
        cx: &mut Context,
        state: &mut Self::PartitionPushState,
        operator_state: &Self::OperatorState,
        input: &mut Batch,
    ) -> Result<PollPush>;

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        state: &mut Self::PartitionPushState,
        operator_state: &Self::OperatorState,
    ) -> Result<PollFinalize>;
}

#[derive(Debug, Clone)]
pub struct RawOperatorState(RawClonePtr);

#[derive(Debug)]
pub struct RawPartitionState(RawPtr);

#[derive(Debug, Clone)]
pub struct PlannedOperatorWithChildren {
    pub operator: PlannedOperator,
    pub children: Vec<PlannedOperatorWithChildren>,
}

impl AsRef<PlannedOperator> for PlannedOperatorWithChildren {
    fn as_ref(&self) -> &PlannedOperator {
        &self.operator
    }
}

#[derive(Debug, Clone)]
pub struct PlannedOperator {
    /// The underlying operator.
    pub(crate) operator: RawClonePtr,
    /// The operator vtable.
    pub(crate) vtable: &'static RawOperatorVTable,
    pub(crate) operator_type: OperatorType,
}

impl PlannedOperator {
    pub fn new_execute<O>(op: O) -> Self
    where
        O: ExecuteOperator,
    {
        unimplemented!()
    }

    pub fn new_push<O>(op: O) -> Self
    where
        O: PushOperator,
    {
        unimplemented!()
    }

    pub fn new_push_execute<O>(op: O) -> Self
    where
        O: PushOperator + ExecuteOperator,
    {
        unimplemented!()
    }

    pub fn new_pull<O>(op: O) -> Self
    where
        O: PullOperator,
    {
        unimplemented!()
    }

    pub fn call_create_operator_state(
        &self,
        context: &DatabaseContext,
        props: ExecutionProperties,
    ) -> Result<RawOperatorState> {
        unimplemented!()
    }

    pub fn call_output_types(&self) -> &[DataType] {
        unimplemented!()
    }

    pub fn call_create_partition_execute_states(
        &self,
        op_state: &RawOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<RawPartitionState>> {
        unimplemented!()
    }

    pub fn call_create_partition_push_states(
        &self,
        op_state: &RawOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<RawPartitionState>> {
        unimplemented!()
    }

    pub fn call_create_partition_pull_states(
        &self,
        op_state: &RawOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<RawPartitionState>> {
        unimplemented!()
    }

    pub fn call_poll_pull(
        &self,
        cx: &mut Context,
        op_state: &RawOperatorState,
        partition_state: &mut RawPartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        unimplemented!()
    }

    pub fn call_poll_push(
        &self,
        cx: &mut Context,
        op_state: &RawOperatorState,
        partition_state: &mut RawPartitionState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    pub fn call_poll_execute(
        &self,
        cx: &mut Context,
        op_state: &RawOperatorState,
        partition_state: &mut RawPartitionState,
        inout: ExecuteInOut,
    ) -> Result<PollExecute> {
        unimplemented!()
    }

    pub fn call_poll_finalize_push(
        &self,
        cx: &mut Context,
        op_state: &RawOperatorState,
        partition_state: &mut RawPartitionState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }

    pub fn call_poll_finalize_execute(
        &self,
        cx: &mut Context,
        op_state: &RawOperatorState,
        partition_state: &mut RawPartitionState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }

    pub fn call_build_pipeline(
        &self,
        db_context: &DatabaseContext,
        props: ExecutionProperties,
        graph: &mut ExecutablePipelineGraph,
        current: &mut ExecutablePipeline,
    ) -> Result<()> {
        unimplemented!()
    }

    pub fn call_explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        unsafe { (self.vtable.explain_fn)(self.operator.get(), conf) }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawOperatorVTable {
    output_types_fn: unsafe fn(operator: *const ()) -> Vec<DataType>,

    poll_push_fn: unsafe fn(
        operator: *const (),
        cx: &mut Context,
        partition_state: *mut (),
        operator_state: *const (),
        input: &mut Batch,
    ) -> Result<PollExecute>,

    poll_execute_fn: unsafe fn(
        operator: *const (),
        cx: &mut Context,
        partition_state: *mut (),
        operator_state: *const (),
        inout: ExecuteInOut,
    ) -> Result<PollExecute>,

    explain_fn: unsafe fn(operator: *const (), conf: ExplainConfig) -> ExplainEntry,
}

pub(crate) trait OperatorVTable {
    const VTABLE: &'static RawOperatorVTable;
}

impl<O> OperatorVTable for O
where
    O: ExecuteOperator,
{
    const VTABLE: &'static RawOperatorVTable = &RawOperatorVTable {
        output_types_fn: |operator| unimplemented!(),
        poll_push_fn: |operator, cx, partition_state, operator_state, input| unimplemented!(),
        poll_execute_fn: |operator, cx, partition_state, operator_state, inout| unimplemented!(),
        explain_fn: |operator, conf| unimplemented!(),
    };
}

// 144 bytes
#[derive(Debug)]
pub enum PhysicalOperator {
    Project(PhysicalProject),
    Filter(PhysicalFilter),
    Limit(PhysicalLimit),
    NestedLoopJoin(PhysicalNestedLoopJoin),
    TableFunction(PhysicalTableFunction),

    HashAggregate(PhysicalHashAggregate),
    UngroupedAggregate(PhysicalUngroupedAggregate),
    Window(PhysicalWindow),
    // HashJoin(PhysicalHashJoin),
    Values(PhysicalValues),
    ResultSink(PhysicalSink<ResultSink>),
    DynSink(PhysicalSink<Box<dyn SinkOperation>>),
    // MaterializedSink(PhysicalSink<MaterializedSinkOperation>),
    // MaterializedSource(PhysicalSource<MaterializeSourceOperation>),
    // MergeSorted(PhysicalGatherSort),
    // LocalSort(PhysicalScatterSort),
    Union(PhysicalUnion),
    Unnest(PhysicalUnnest),
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
    type States = PartitionAndOperatorStates;

    fn output_types(&self) -> &[DataType] {
        unimplemented!()
    }

    fn create_states(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<PartitionAndOperatorStates> {
        unimplemented!()
    }

    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }
}

impl Explainable for PhysicalOperator {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        unimplemented!()
    }
}
