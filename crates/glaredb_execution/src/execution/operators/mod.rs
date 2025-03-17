//! Implementations of physical operators in an execution pipeline.

pub mod catalog;
pub mod empty;
pub mod filter;
pub mod hash_aggregate;
pub mod hash_join;
pub mod limit;
pub mod materialize;
pub mod nested_loop_join;
pub mod project;
pub mod results;
pub mod scan;
pub mod sort;
pub mod table_execute;
pub mod ungrouped_aggregate;
pub mod values;

pub(crate) mod util;

use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::task::Context;

use glaredb_error::{RayexecError, Result};

use super::pipeline::{ExecutablePipeline, ExecutablePipelineGraph};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::logical::binder::bind_context::MaterializationRef;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionProperties {
    /// Batch size, in rows, that were working with.
    pub batch_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorType {
    /// Unary operator.
    Execute,
    /// Source operator.
    Pull,
    /// Sink operator.
    Push,
    /// Binary operator, with left side terminating.
    PushExecute,
    /// Acts a sink for a single pipeline, and a source for some number of other
    /// pipelines.
    Materializing,
}

impl OperatorType {
    pub const fn children_requirement(&self) -> usize {
        match self {
            Self::Execute | Self::Push => 1,
            Self::Pull => 0,
            Self::PushExecute => 2,
            // When reaching a materialize node during pipeline planning, the
            // children should not be on the materialization. Children planning
            // happens before we build the pipelines for the rest of the query.
            //
            // Instead we'll reach into the pipeline graph to get the correct
            // materialization operator (and state) to use, letting us share the
            // operator state between the sink side and any number of the source
            // sides.
            Self::Materializing => 0,
        }
    }
}

pub trait BaseOperator: Sync + Send + Debug + Explainable + 'static {
    type OperatorState: Sync + Send;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState>;

    fn output_types(&self) -> &[DataType];

    fn build_pipeline(
        operator: &PlannedOperator,
        children: &[PlannedOperatorWithChildren],
        props: ExecutionProperties,
        graph: &mut ExecutablePipelineGraph,
        current: &mut ExecutablePipeline,
    ) -> Result<()> {
        if children.len() != operator.operator_type.children_requirement() {
            return Err(RayexecError::new("Unexpected number of children"));
        }

        match operator.operator_type {
            OperatorType::Execute | OperatorType::Push => {
                // Plan child operator.
                let child = &children[0];
                child.build_pipeline(props, graph, current)?;

                // Push self onto pipeline.
                let state = operator.call_create_operator_state(props)?;
                current.push_operator_and_state(operator.clone(), state);

                Ok(())
            }
            OperatorType::Pull => {
                // No children, just push self.
                let state = operator.call_create_operator_state(props)?;
                current.push_operator_and_state(operator.clone(), state);

                Ok(())
            }
            OperatorType::Materializing => {
                // Materializing operator, we can assume this is a scan since
                // the push side should have been handled prior to calling this
                // operator's build_pipeline method.
                //
                // Reach into the query graph to get the already planned
                // operator and state.
                //
                // Note that we discard this operator as it's essentially just a
                // marker return the materialization reference to use.
                let mat_ref = operator.call_materialization_ref()?;

                let mat = match graph.materializations.get(&mat_ref) {
                    Some(mat) => mat,
                    None => {
                        return Err(RayexecError::new(format!(
                            "Missing executable materialization for ref: {mat_ref}"
                        )));
                    }
                };

                current.push_operator_and_state(mat.operator.clone(), mat.operator_state.clone());

                Ok(())
            }
            OperatorType::PushExecute => {
                // Create new pipeline for left/push child.
                let mut left_pipeline = ExecutablePipeline::default();
                let left_child = &children[0];
                left_child.build_pipeline(props, graph, &mut left_pipeline)?;

                // Now build up the right/execute child using the current
                // pipeline.
                let right_child = &children[1];
                right_child.build_pipeline(props, graph, current)?;

                // Push operator and state to both pipelines.
                let state = operator.call_create_operator_state(props)?;
                left_pipeline.push_operator_and_state(operator.clone(), state.clone());
                current.push_operator_and_state(operator.clone(), state.clone());

                // Left pipeline finished, push to query graph.
                graph.push_pipeline(left_pipeline);

                Ok(())
            }
        }
    }
}

pub trait MaterializingOperator: PushOperator + PullOperator {
    /// Return the materialization reference associated with this
    /// materialization.
    fn materialization_ref(&self) -> MaterializationRef;
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
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute>;

    fn poll_finalize_execute(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
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
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPullState,
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
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
        input: &mut Batch,
    ) -> Result<PollPush>;

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize>;
}

#[derive(Debug, Clone)]
pub struct AnyOperatorState(Arc<dyn Any + Sync + Send>);

#[derive(Debug)]
pub struct AnyPartitionState(Box<dyn Any + Sync + Send>);

#[derive(Debug, Clone)]
pub struct PlannedOperatorWithChildren {
    pub operator: PlannedOperator,
    pub children: Vec<PlannedOperatorWithChildren>,
}

impl PlannedOperatorWithChildren {
    pub fn build_pipeline(
        &self,
        props: ExecutionProperties,
        graph: &mut ExecutablePipelineGraph,
        current: &mut ExecutablePipeline,
    ) -> Result<()> {
        self.operator
            .call_build_pipeline(&self.children, props, graph, current)
    }
}

#[derive(Debug, Clone)]
pub struct PlannedOperator {
    /// The underlying operator.
    pub(crate) operator: Arc<dyn Any + Sync + Send>,
    /// The operator vtable.
    pub(crate) vtable: &'static RawOperatorVTable,
    pub(crate) operator_type: OperatorType,
}

impl PlannedOperator {
    pub fn new_execute<O>(op: O) -> Self
    where
        O: ExecuteOperator,
    {
        PlannedOperator {
            operator: Arc::new(op),
            vtable: ExecuteOperatorVTable::<O>::VTABLE,
            operator_type: ExecuteOperatorVTable::<O>::OPERATOR_TYPE,
        }
    }

    pub fn new_push<O>(op: O) -> Self
    where
        O: PushOperator,
    {
        PlannedOperator {
            operator: Arc::new(op),
            vtable: PushOperatorVTable::<O>::VTABLE,
            operator_type: PushOperatorVTable::<O>::OPERATOR_TYPE,
        }
    }

    pub fn new_push_execute<O>(op: O) -> Self
    where
        O: PushOperator + ExecuteOperator,
    {
        PlannedOperator {
            operator: Arc::new(op),
            vtable: PushExecuteOperatorVTable::<O>::VTABLE,
            operator_type: PushExecuteOperatorVTable::<O>::OPERATOR_TYPE,
        }
    }

    pub fn new_pull<O>(op: O) -> Self
    where
        O: PullOperator,
    {
        PlannedOperator {
            operator: Arc::new(op),
            vtable: PullOperatorVTable::<O>::VTABLE,
            operator_type: PullOperatorVTable::<O>::OPERATOR_TYPE,
        }
    }

    pub fn new_materializing<O>(op: O) -> Self
    where
        O: MaterializingOperator,
    {
        PlannedOperator {
            operator: Arc::new(op),
            vtable: MaterializingOperatorVTable::<O>::VTABLE,
            operator_type: MaterializingOperatorVTable::<O>::OPERATOR_TYPE,
        }
    }

    pub fn call_create_operator_state(
        &self,
        props: ExecutionProperties,
    ) -> Result<AnyOperatorState> {
        unsafe { (self.vtable.create_operator_state_fn)(self.operator.as_ref(), props) }
    }

    pub fn call_output_types(&self) -> Vec<DataType> {
        unsafe { (self.vtable.output_types_fn)(self.operator.as_ref()) }
    }

    pub fn call_materialization_ref(&self) -> Result<MaterializationRef> {
        unsafe { (self.vtable.materialization_ref_fn)(self.operator.as_ref()) }
    }

    pub fn call_create_partition_execute_states(
        &self,
        op_state: &AnyOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyPartitionState>> {
        unsafe {
            (self.vtable.create_partition_execute_states_fn)(
                self.operator.as_ref(),
                op_state.0.as_ref(),
                props,
                partitions,
            )
        }
    }

    pub fn call_create_partition_push_states(
        &self,
        op_state: &AnyOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyPartitionState>> {
        unsafe {
            (self.vtable.create_partition_push_states_fn)(
                self.operator.as_ref(),
                op_state.0.as_ref(),
                props,
                partitions,
            )
        }
    }

    pub fn call_create_partition_pull_states(
        &self,
        op_state: &AnyOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyPartitionState>> {
        unsafe {
            (self.vtable.create_partition_pull_states_fn)(
                self.operator.as_ref(),
                op_state.0.as_ref(),
                props,
                partitions,
            )
        }
    }

    pub fn call_poll_pull(
        &self,
        cx: &mut Context,
        op_state: &AnyOperatorState,
        partition_state: &mut AnyPartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        unsafe {
            (self.vtable.poll_pull_fn)(
                self.operator.as_ref(),
                cx,
                op_state.0.as_ref(),
                partition_state.0.as_mut(),
                output,
            )
        }
    }

    pub fn call_poll_push(
        &self,
        cx: &mut Context,
        op_state: &AnyOperatorState,
        partition_state: &mut AnyPartitionState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        unsafe {
            (self.vtable.poll_push_fn)(
                self.operator.as_ref(),
                cx,
                op_state.0.as_ref(),
                partition_state.0.as_mut(),
                input,
            )
        }
    }

    pub fn call_poll_execute(
        &self,
        cx: &mut Context,
        op_state: &AnyOperatorState,
        partition_state: &mut AnyPartitionState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        unsafe {
            (self.vtable.poll_execute_fn)(
                self.operator.as_ref(),
                cx,
                op_state.0.as_ref(),
                partition_state.0.as_mut(),
                input,
                output,
            )
        }
    }

    pub fn call_poll_finalize_push(
        &self,
        cx: &mut Context,
        op_state: &AnyOperatorState,
        partition_state: &mut AnyPartitionState,
    ) -> Result<PollFinalize> {
        unsafe {
            (self.vtable.poll_finalize_push_fn)(
                self.operator.as_ref(),
                cx,
                op_state.0.as_ref(),
                partition_state.0.as_mut(),
            )
        }
    }

    pub fn call_poll_finalize_execute(
        &self,
        cx: &mut Context,
        op_state: &AnyOperatorState,
        partition_state: &mut AnyPartitionState,
    ) -> Result<PollFinalize> {
        unsafe {
            (self.vtable.poll_finalize_execute_fn)(
                self.operator.as_ref(),
                cx,
                op_state.0.as_ref(),
                partition_state.0.as_mut(),
            )
        }
    }

    pub fn call_build_pipeline(
        &self,
        children: &[PlannedOperatorWithChildren],
        props: ExecutionProperties,
        graph: &mut ExecutablePipelineGraph,
        current: &mut ExecutablePipeline,
    ) -> Result<()> {
        unsafe { (self.vtable.build_pipeline_fn)(self, children, props, graph, current) }
    }

    pub fn call_explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        unsafe { (self.vtable.explain_fn)(self.operator.as_ref(), conf) }
    }
}

#[derive(Debug, Clone, Copy)]
#[allow(clippy::type_complexity)]
pub(crate) struct RawOperatorVTable {
    create_operator_state_fn:
        unsafe fn(operator: &dyn Any, props: ExecutionProperties) -> Result<AnyOperatorState>,

    output_types_fn: unsafe fn(operator: &dyn Any) -> Vec<DataType>,

    materialization_ref_fn: unsafe fn(operator: &dyn Any) -> Result<MaterializationRef>,

    build_pipeline_fn: unsafe fn(
        operator: &PlannedOperator,
        children: &[PlannedOperatorWithChildren],
        props: ExecutionProperties,
        graph: &mut ExecutablePipelineGraph,
        current: &mut ExecutablePipeline,
    ) -> Result<()>,

    create_partition_execute_states_fn: unsafe fn(
        operator: &dyn Any,
        operator_state: &dyn Any,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyPartitionState>>,

    create_partition_pull_states_fn: unsafe fn(
        operator: &dyn Any,
        operator_state: &dyn Any,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyPartitionState>>,

    create_partition_push_states_fn: unsafe fn(
        operator: &dyn Any,
        operator_state: &dyn Any,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyPartitionState>>,

    poll_push_fn: unsafe fn(
        operator: &dyn Any,
        cx: &mut Context,
        operator_state: &dyn Any,
        partition_state: &mut dyn Any,
        input: &mut Batch,
    ) -> Result<PollPush>,

    poll_execute_fn: unsafe fn(
        operator: &dyn Any,
        cx: &mut Context,
        operator_state: &dyn Any,
        partition_state: &mut dyn Any,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute>,

    poll_pull_fn: unsafe fn(
        operator: &dyn Any,
        cx: &mut Context,
        operator_state: &dyn Any,
        partition_state: &mut dyn Any,
        ouput: &mut Batch,
    ) -> Result<PollPull>,

    poll_finalize_execute_fn: unsafe fn(
        operator: &dyn Any,
        cx: &mut Context,
        operator_state: &dyn Any,
        partition_state: &mut dyn Any,
    ) -> Result<PollFinalize>,

    poll_finalize_push_fn: unsafe fn(
        operator: &dyn Any,
        cx: &mut Context,
        operator_state: &dyn Any,
        partition_state: &mut dyn Any,
    ) -> Result<PollFinalize>,

    explain_fn: unsafe fn(operator: &dyn Any, conf: ExplainConfig) -> ExplainEntry,
}

/// Helper trait for creating the vtable for operators.
trait OperatorVTable {
    const OPERATOR_TYPE: OperatorType;
    const VTABLE: &'static RawOperatorVTable;
}

// TODO: Definitely a bit of repeated code, but currently pretty easy to see at
// a glance what's going on.
//
// It may make sense to condense this down if necessary.

struct ExecuteOperatorVTable<O: ExecuteOperator>(PhantomData<O>);

impl<O> OperatorVTable for ExecuteOperatorVTable<O>
where
    O: ExecuteOperator,
{
    const OPERATOR_TYPE: OperatorType = OperatorType::Execute;

    const VTABLE: &'static RawOperatorVTable = &RawOperatorVTable {
        create_operator_state_fn: |operator, props| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = operator.create_operator_state(props)?;
            Ok(AnyOperatorState(Arc::new(state)))
        },

        output_types_fn: |operator| {
            let operator = operator.downcast_ref::<O>().unwrap();
            operator.output_types().to_vec()
        },

        materialization_ref_fn: |_operator| Err(RayexecError::new("Not a materializing operator")),

        build_pipeline_fn: |operator, children, props, graph, current| {
            O::build_pipeline(operator, children, props, graph, current)
        },

        create_partition_execute_states_fn: |operator, op_state, props, partitions| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            let states = operator.create_partition_execute_states(op_state, props, partitions)?;
            Ok(states
                .into_iter()
                .map(|state| AnyPartitionState(Box::new(state)))
                .collect())
        },

        create_partition_pull_states_fn: |_operator, _op_state, _props, _partitions| {
            Err(RayexecError::new("Not a pull operator"))
        },
        create_partition_push_states_fn: |_operator, _op_state, _props, _partitions| {
            Err(RayexecError::new("Not a push operator"))
        },

        poll_pull_fn: |_operator, _cx, _partition_state, _operator_state, _output| {
            Err(RayexecError::new("Not a pull operator"))
        },
        poll_push_fn: |_operator, _cx, _partition_state, _operator_state, _input| {
            Err(RayexecError::new("Not a push operator"))
        },
        poll_execute_fn: |operator, cx, op_state, partition_state, input, output| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as ExecuteOperator>::PartitionExecuteState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            operator.poll_execute(cx, op_state, state, input, output)
        },

        poll_finalize_push_fn: |_operator, _cx, _partition_state, _operator_state| {
            Err(RayexecError::new("Not a push operator"))
        },
        poll_finalize_execute_fn: |operator, cx, op_state, partition_state| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as ExecuteOperator>::PartitionExecuteState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            operator.poll_finalize_execute(cx, op_state, state)
        },

        explain_fn: |operator, conf| {
            let operator = operator.downcast_ref::<O>().unwrap();
            operator.explain_entry(conf)
        },
    };
}

struct PushOperatorVTable<O: PushOperator>(PhantomData<O>);

impl<O> OperatorVTable for PushOperatorVTable<O>
where
    O: PushOperator,
{
    const OPERATOR_TYPE: OperatorType = OperatorType::Push;

    const VTABLE: &'static RawOperatorVTable = &RawOperatorVTable {
        create_operator_state_fn: |operator, props| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = operator.create_operator_state(props)?;
            Ok(AnyOperatorState(Arc::new(state)))
        },

        output_types_fn: |operator| {
            let operator = operator.downcast_ref::<O>().unwrap();
            operator.output_types().to_vec()
        },

        materialization_ref_fn: |_operator| Err(RayexecError::new("Not a materializing operator")),

        build_pipeline_fn: |operator, children, props, graph, current| {
            O::build_pipeline(operator, children, props, graph, current)
        },

        create_partition_execute_states_fn: |_operator, _op_state, _props, _partitions| {
            Err(RayexecError::new("Not an execute operator"))
        },

        create_partition_pull_states_fn: |_operator, _op_state, _props, _partitions| {
            Err(RayexecError::new("Not a pull operator"))
        },
        create_partition_push_states_fn: |operator, op_state, props, partitions| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            let states = operator.create_partition_push_states(op_state, props, partitions)?;
            Ok(states
                .into_iter()
                .map(|state| AnyPartitionState(Box::new(state)))
                .collect())
        },

        poll_pull_fn: |_operator, _cx, _partition_state, _operator_state, _output| {
            Err(RayexecError::new("Not a pull operator"))
        },
        poll_push_fn: |operator, cx, op_state, partition_state, input| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as PushOperator>::PartitionPushState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            operator.poll_push(cx, op_state, state, input)
        },
        poll_execute_fn: |_operator, _cx, _partition_state, _operator_state, _input, _output| {
            Err(RayexecError::new("Not an execute operator"))
        },

        poll_finalize_push_fn: |operator, cx, op_state, partition_state| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as PushOperator>::PartitionPushState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            operator.poll_finalize_push(cx, op_state, state)
        },
        poll_finalize_execute_fn: |_operator, _cx, _partition_state, _operator_state| {
            Err(RayexecError::new("Not an execute operator"))
        },

        explain_fn: |operator, conf| {
            let operator = operator.downcast_ref::<O>().unwrap();
            operator.explain_entry(conf)
        },
    };
}

struct PushExecuteOperatorVTable<O: ExecuteOperator + PushOperator>(PhantomData<O>);

impl<O> OperatorVTable for PushExecuteOperatorVTable<O>
where
    O: ExecuteOperator + PushOperator,
{
    const OPERATOR_TYPE: OperatorType = OperatorType::PushExecute;

    const VTABLE: &'static RawOperatorVTable = &RawOperatorVTable {
        create_operator_state_fn: |operator, props| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = operator.create_operator_state(props)?;
            Ok(AnyOperatorState(Arc::new(state)))
        },

        output_types_fn: |operator| {
            let operator = operator.downcast_ref::<O>().unwrap();
            operator.output_types().to_vec()
        },

        materialization_ref_fn: |_operator| Err(RayexecError::new("Not a materializing operator")),

        build_pipeline_fn: |operator, children, props, graph, current| {
            O::build_pipeline(operator, children, props, graph, current)
        },

        create_partition_execute_states_fn: |operator, op_state, props, partitions| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            let states = operator.create_partition_execute_states(op_state, props, partitions)?;
            Ok(states
                .into_iter()
                .map(|state| AnyPartitionState(Box::new(state)))
                .collect())
        },

        create_partition_pull_states_fn: |_operator, _op_state, _props, _partitions| {
            Err(RayexecError::new("Not a pull operator"))
        },
        create_partition_push_states_fn: |operator, op_state, props, partitions| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            let states = operator.create_partition_push_states(op_state, props, partitions)?;
            Ok(states
                .into_iter()
                .map(|state| AnyPartitionState(Box::new(state)))
                .collect())
        },

        poll_pull_fn: |_operator, _cx, _partition_state, _operator_state, _output| {
            Err(RayexecError::new("Not a pull operator"))
        },
        poll_push_fn: |operator, cx, op_state, partition_state, input| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as PushOperator>::PartitionPushState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();

            operator.poll_push(cx, op_state, state, input)
        },
        poll_execute_fn: |operator, cx, op_state, partition_state, input, output| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as ExecuteOperator>::PartitionExecuteState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            operator.poll_execute(cx, op_state, state, input, output)
        },

        poll_finalize_push_fn: |operator, cx, op_state, partition_state| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as PushOperator>::PartitionPushState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();

            operator.poll_finalize_push(cx, op_state, state)
        },
        poll_finalize_execute_fn: |operator, cx, op_state, partition_state| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as ExecuteOperator>::PartitionExecuteState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();

            operator.poll_finalize_execute(cx, op_state, state)
        },

        explain_fn: |operator, conf| {
            let operator = operator.downcast_ref::<O>().unwrap();
            operator.explain_entry(conf)
        },
    };
}

struct PullOperatorVTable<O: PullOperator>(PhantomData<O>);

impl<O> OperatorVTable for PullOperatorVTable<O>
where
    O: PullOperator,
{
    const OPERATOR_TYPE: OperatorType = OperatorType::Pull;

    const VTABLE: &'static RawOperatorVTable = &RawOperatorVTable {
        create_operator_state_fn: |operator, props| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = operator.create_operator_state(props)?;
            Ok(AnyOperatorState(Arc::new(state)))
        },

        output_types_fn: |operator| {
            let operator = operator.downcast_ref::<O>().unwrap();
            operator.output_types().to_vec()
        },

        materialization_ref_fn: |_operator| Err(RayexecError::new("Not a materializing operator")),

        build_pipeline_fn: |operator, children, props, graph, current| {
            O::build_pipeline(operator, children, props, graph, current)
        },

        create_partition_execute_states_fn: |_operator, _op_state, _props, _partitions| {
            Err(RayexecError::new("Not an execute operator"))
        },

        create_partition_pull_states_fn: |operator, op_state, props, partitions| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            let states = operator.create_partition_pull_states(op_state, props, partitions)?;
            Ok(states
                .into_iter()
                .map(|state| AnyPartitionState(Box::new(state)))
                .collect())
        },
        create_partition_push_states_fn: |_operator, _op_state, _props, _partitions| {
            Err(RayexecError::new("Not a push operator"))
        },

        poll_pull_fn: |operator, cx, op_state, partition_state, output| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as PullOperator>::PartitionPullState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            operator.poll_pull(cx, op_state, state, output)
        },
        poll_push_fn: |_operator, _cx, _partition_state, _operator_state, _input| {
            Err(RayexecError::new("Not a push operator"))
        },
        poll_execute_fn: |_operator, _cx, _partition_state, _operator_state, _input, _output| {
            Err(RayexecError::new("Not an execute operator"))
        },

        poll_finalize_push_fn: |_operator, _cx, _partition_state, _operator_state| {
            Err(RayexecError::new("Not a push operator"))
        },
        poll_finalize_execute_fn: |_operator, _cx, _partition_state, _operator_state| {
            Err(RayexecError::new("Not an execute operator"))
        },

        explain_fn: |operator, conf| {
            let operator = operator.downcast_ref::<O>().unwrap();
            operator.explain_entry(conf)
        },
    };
}

struct MaterializingOperatorVTable<O: MaterializingOperator>(PhantomData<O>);

impl<O> OperatorVTable for MaterializingOperatorVTable<O>
where
    O: MaterializingOperator,
{
    const OPERATOR_TYPE: OperatorType = OperatorType::Materializing;

    const VTABLE: &'static RawOperatorVTable = &RawOperatorVTable {
        create_operator_state_fn: |operator, props| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = operator.create_operator_state(props)?;
            Ok(AnyOperatorState(Arc::new(state)))
        },

        output_types_fn: |operator| {
            let operator = operator.downcast_ref::<O>().unwrap();
            operator.output_types().to_vec()
        },

        materialization_ref_fn: |operator| {
            let operator = operator.downcast_ref::<O>().unwrap();
            Ok(operator.materialization_ref())
        },

        build_pipeline_fn: |operator, children, props, graph, current| {
            O::build_pipeline(operator, children, props, graph, current)
        },

        create_partition_execute_states_fn: |_operator, _op_state, _props, _partitions| {
            Err(RayexecError::new("Not an execute operator"))
        },

        create_partition_pull_states_fn: |operator, op_state, props, partitions| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            let states = operator.create_partition_pull_states(op_state, props, partitions)?;
            Ok(states
                .into_iter()
                .map(|state| AnyPartitionState(Box::new(state)))
                .collect())
        },
        create_partition_push_states_fn: |operator, op_state, props, partitions| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            let states = operator.create_partition_push_states(op_state, props, partitions)?;
            Ok(states
                .into_iter()
                .map(|state| AnyPartitionState(Box::new(state)))
                .collect())
        },

        poll_pull_fn: |operator, cx, op_state, partition_state, output| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as PullOperator>::PartitionPullState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();
            operator.poll_pull(cx, op_state, state, output)
        },
        poll_push_fn: |operator, cx, op_state, partition_state, input| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as PushOperator>::PartitionPushState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();

            operator.poll_push(cx, op_state, state, input)
        },
        poll_execute_fn: |_operator, _cx, _partition_state, _operator_state, _input, _output| {
            Err(RayexecError::new("Not an execute operator"))
        },
        poll_finalize_push_fn: |operator, cx, op_state, partition_state| {
            let operator = operator.downcast_ref::<O>().unwrap();
            let state = partition_state
                .downcast_mut::<<O as PushOperator>::PartitionPushState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<O as BaseOperator>::OperatorState>()
                .unwrap();

            operator.poll_finalize_push(cx, op_state, state)
        },
        poll_finalize_execute_fn: |_operator, _cx, _partition_state, _operator_state| {
            Err(RayexecError::new("Not an execute operator"))
        },

        explain_fn: |operator, conf| {
            let operator = operator.downcast_ref::<O>().unwrap();
            operator.explain_entry(conf)
        },
    };
}
