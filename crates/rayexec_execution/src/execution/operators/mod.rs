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
use rayexec_error::{not_implemented, OptionExt, Result};
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
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;
use crate::engine::result::ResultSink;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

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
        inout: ExecuteInOutState,
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
        match self {
            Self::HashAggregate(op) => op.output_types(),
            Self::UngroupedAggregate(op) => op.output_types(),
            Self::Window(op) => op.output_types(),
            Self::NestedLoopJoin(op) => op.output_types(),
            // Self::HashJoin(op) => op.output_types(),
            Self::Values(op) => op.output_types(),
            Self::ResultSink(op) => op.output_types(),
            Self::DynSink(op) => op.output_types(),
            // Self::MaterializedSink(op) => op.output_types(),
            // Self::MaterializedSource(op) => op.output_types(),
            // Self::MergeSorted(op) => op.output_types(),
            // Self::LocalSort(op) => op.output_types(),
            Self::Limit(op) => op.output_types(),
            Self::Union(op) => op.output_types(),
            Self::Filter(op) => op.output_types(),
            Self::Project(op) => op.output_types(),
            Self::Unnest(op) => op.output_types(),
            Self::TableFunction(op) => op.output_types(),
            Self::TableInOut(op) => op.output_types(),
            Self::Insert(op) => op.output_types(),
            Self::CopyTo(op) => op.output_types(),
            Self::CreateTable(op) => op.output_types(),
            Self::CreateSchema(op) => op.output_types(),
            Self::CreateView(op) => op.output_types(),
            Self::Drop(op) => op.output_types(),
            Self::Empty(op) => op.output_types(),
        }
    }

    fn create_states(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<PartitionAndOperatorStates> {
        let states = match self {
            Self::HashAggregate(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::UngroupedAggregate(op) => {
                op.create_states(context, batch_size, partitions)?.into()
            }
            Self::Window(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::NestedLoopJoin(op) => op.create_states(context, batch_size, partitions)?.into(),
            // Self::HashJoin(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::Values(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::ResultSink(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::DynSink(op) => op.create_states(context, batch_size, partitions)?.into(),
            // Self::MaterializedSink(op) => op.create_states(context, batch_size, partitions)?.into(),
            // Self::MaterializedSource(op) => {
            //     op.create_states(context, batch_size, partitions)?.into()
            // }
            // Self::MergeSorted(op) => op.create_states(context, batch_size, partitions)?.into(),
            // Self::LocalSort(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::Limit(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::Union(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::Filter(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::Project(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::Unnest(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::TableFunction(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::TableInOut(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::Insert(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::CopyTo(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::CreateTable(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::CreateSchema(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::CreateView(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::Drop(op) => op.create_states(context, batch_size, partitions)?.into(),
            Self::Empty(op) => op.create_states(context, batch_size, partitions)?.into(),
        };

        Ok(states)
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
            // Self::HashJoin(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Values(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::ResultSink(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::DynSink(op) => op.poll_finalize(cx, partition_state, operator_state),
            // Self::MaterializedSink(op) => op.poll_finalize(cx, partition_state, operator_state),
            // Self::MaterializedSource(op) => op.poll_finalize(cx, partition_state, operator_state),
            // Self::MergeSorted(op) => op.poll_finalize(cx, partition_state, operator_state),
            // Self::LocalSort(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Limit(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Union(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Filter(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Project(op) => op.poll_finalize(cx, partition_state, operator_state),
            Self::Unnest(op) => op.poll_finalize(cx, partition_state, operator_state),
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
}

impl Explainable for PhysicalOperator {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        match self {
            Self::HashAggregate(op) => op.explain_entry(conf),
            Self::UngroupedAggregate(op) => op.explain_entry(conf),
            Self::Window(op) => op.explain_entry(conf),
            Self::NestedLoopJoin(op) => op.explain_entry(conf),
            // Self::HashJoin(op) => op.explain_entry(conf),
            Self::Values(op) => op.explain_entry(conf),
            Self::ResultSink(op) => op.explain_entry(conf),
            Self::DynSink(op) => op.explain_entry(conf),
            // Self::MaterializedSink(op) => op.explain_entry(conf),
            // Self::MaterializedSource(op) => op.explain_entry(conf),
            // Self::MergeSorted(op) => op.explain_entry(conf),
            // Self::LocalSort(op) => op.explain_entry(conf),
            Self::Limit(op) => op.explain_entry(conf),
            Self::Union(op) => op.explain_entry(conf),
            Self::Filter(op) => op.explain_entry(conf),
            Self::Project(op) => op.explain_entry(conf),
            Self::Unnest(op) => op.explain_entry(conf),
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
            // Self::UngroupedAggregate(op) => Value::UngroupedAggregate(op.to_proto_ctx(context)?),
            Self::Union(op) => Value::Union(op.to_proto_ctx(context)?),
            Self::Values(op) => Value::Values(op.to_proto_ctx(context)?),
            // Self::TableFunction(op) => Value::TableFunction(op.to_proto_ctx(context)?),
            // Self::NestedLoopJoin(op) => Value::NlJoin(op.to_proto_ctx(context)?),
            Self::CopyTo(op) => Value::CopyTo(op.to_proto_ctx(context)?),
            // Self::LocalSort(op) => Value::LocalSort(op.to_proto_ctx(context)?),
            // Self::MergeSorted(op) => Value::MergeSorted(op.to_proto_ctx(context)?),
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
            //     PhysicalOperator::Filter(PhysicalFilter::from_proto_ctx(op, context)?)
            // }
            // Value::Project(op) => {
            //     PhysicalOperator::Project(PhysicalProject::from_proto_ctx(op, context)?)
            // }
            Value::Insert(op) => {
                PhysicalOperator::Insert(PhysicalInsert::from_proto_ctx(op, context)?)
            }
            Value::Limit(op) => {
                PhysicalOperator::Limit(PhysicalLimit::from_proto_ctx(op, context)?)
            }
            // Value::UngroupedAggregate(op) => PhysicalOperator::UngroupedAggregate(
            //     PhysicalUngroupedAggregate::from_proto_ctx(op, context)?,
            // ),
            Value::Union(op) => {
                PhysicalOperator::Union(PhysicalUnion::from_proto_ctx(op, context)?)
            }
            Value::Values(op) => {
                PhysicalOperator::Values(PhysicalValues::from_proto_ctx(op, context)?)
            }
            // Value::TableFunction(op) => {
            //     PhysicalOperator::TableFunction(PhysicalTableFunction::from_proto_ctx(op, context)?)
            // }
            // Value::NlJoin(op) => PhysicalOperator::NestedLoopJoin(
            //     PhysicalNestedLoopJoin::from_proto_ctx(op, context)?,
            // ),
            Value::CopyTo(op) => {
                PhysicalOperator::CopyTo(PhysicalCopyTo::from_proto_ctx(op, context)?)
            }
            // Value::LocalSort(op) => {
            //     PhysicalOperator::LocalSort(PhysicalScatterSort::from_proto_ctx(op, context)?)
            // }
            // Value::MergeSorted(op) => {
            //     PhysicalOperator::MergeSorted(PhysicalGatherSort::from_proto_ctx(op, context)?)
            // }
            _ => unimplemented!(),
        })
    }
}
