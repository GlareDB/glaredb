use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use rayexec_error::{RayexecError, Result};
use tracing::trace;

use super::profiler::OperatorProfileData;
use super::stack::{ExecutionStack, StackEffectHandler};
use crate::arrays::batch::Batch;
use crate::execution::computed_batch::ComputedBatches;
use crate::execution::executable::stack::StackControlFlow;
use crate::execution::operators::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PhysicalOperator,
    PollExecute,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::runtime::time::{RuntimeInstant, Timer};

// TODO: Include intermedate pipeline to track lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipelineId(pub usize);

/// A pipeline represents execution across a sequence of operators.
///
/// Pipelines are made of multiple partition pipelines, where all partition
/// pipelines are doing the same work across the same operators, just in a
/// different partition.
#[derive(Debug)]
pub struct ExecutablePipeline {
    /// ID of this pipeline. Unique to the query graph.
    ///
    /// Informational only.
    #[allow(dead_code)]
    pub(crate) pipeline_id: PipelineId,
    /// Parition pipelines that make up this pipeline.
    pub(crate) partitions: Vec<ExecutablePartitionPipeline>,
}

impl ExecutablePipeline {
    pub(crate) fn new(pipeline_id: PipelineId, num_partitions: usize) -> Self {
        assert_ne!(0, num_partitions);
        let partitions = (0..num_partitions)
            .map(|partition| ExecutablePartitionPipeline::new(pipeline_id, partition))
            .collect();
        ExecutablePipeline {
            pipeline_id,
            partitions,
        }
    }

    /// Return number of partitions in this pipeline.
    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    /// Return the number of operators in this pipeline.
    pub fn num_operators(&self) -> usize {
        self.partitions
            .first()
            .expect("at least one partition")
            .operators
            .len()
    }

    pub fn into_partition_pipeline_iter(self) -> impl Iterator<Item = ExecutablePartitionPipeline> {
        self.partitions.into_iter()
    }

    /// Push an operator onto the pipeline.
    ///
    /// This will push the operator along with its state onto each of the inner
    /// partition pipelines.
    ///
    /// `partition_states` are the unique states per partition and must equal
    /// the number of partitions in this pipeline.
    pub(crate) fn push_operator(
        &mut self,
        physical: Arc<PhysicalOperator>,
        operator_state: Arc<OperatorState>,
        partition_states: Vec<PartitionState>,
    ) -> Result<()> {
        if partition_states.len() != self.num_partitions() {
            return Err(RayexecError::new(format!(
                "Invalid number of partition states, got: {}, expected: {}",
                partition_states.len(),
                self.num_partitions()
            )));
        }

        unimplemented!()
        // let operators = partition_states
        //     .into_iter()
        //     .map(|partition_state| OperatorWithState {
        //         physical: physical.clone(),
        //         operator_state: operator_state.clone(),
        //         partition_state,
        //         profile_data: OperatorProfileData::default(),
        //     });

        // for (operator, partition_pipeline) in operators.zip(self.partitions.iter_mut()) {
        //     partition_pipeline.operators.push(operator)
        // }

        // Ok(())
    }
}

impl Explainable for ExecutablePipeline {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(format!("Pipeline {}", self.pipeline_id.0))
    }
}

/// A partition pipeline holds a sequence of operators along with the state for
/// a single partition.
///
/// This is the smallest unit of work as it relates to the scheduler.
#[derive(Debug)]
pub struct ExecutablePartitionPipeline {
    /// Information about the pipeline.
    ///
    /// Should only be used for generating profiling data.
    info: PartitionPipelineInfo,
    /// All operators part of this pipeline.
    ///
    /// Data batches flow from left to right.
    ///
    /// The left-most operator will only be pulled from, while the right most
    /// will only pushed to.
    operators: Vec<OperatorWithState>,
    /// Instruction stack for how to execute the operators.
    stack: ExecutionStack,
}

impl ExecutablePartitionPipeline {
    fn new(pipeline: PipelineId, partition: usize) -> Self {
        unimplemented!()
        // ExecutablePartitionPipeline {
        //     info: PartitionPipelineInfo {
        //         pipeline,
        //         partition,
        //     },
        //     operators: Vec::new(),
        //     execute_start: 0,
        //     state: PipelineState::Executing(PipelineExecutingState {
        //         current: 0,
        //         execute_start: 0,
        //     }),
        // }
    }

    /// Get the pipeline id for this partition pipeline.
    pub fn pipeline_id(&self) -> PipelineId {
        self.info.pipeline
    }

    /// Get the partition number for this partition pipeline.
    pub fn partition(&self) -> usize {
        self.info.partition
    }

    pub fn operators(&self) -> &[OperatorWithState] {
        &self.operators
    }
}

/// Information about a partition pipeline.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PartitionPipelineInfo {
    pub pipeline: PipelineId,
    pub partition: usize,
}

#[derive(Debug)]
pub struct OperatorWithState {
    /// The underlying physical operator.
    physical: Arc<PhysicalOperator>,
    /// The state that's shared across all partitions for this operator.
    operator_state: Arc<OperatorState>,
    /// States for this operator that's exclusive to this partition.
    partition_states: PartitionStates,
    /// Output batch buffer for this operator/pipeline.
    ///
    /// May be None if this operator is the last in the pipeline.
    output_buffer: Option<Batch>,
    /// Profile data for this operator.
    profile_data: OperatorProfileData,
}

impl OperatorWithState {
    pub fn physical_operator(&self) -> &dyn ExecutableOperator {
        self.physical.as_ref()
    }

    pub fn profile_data(&self) -> &OperatorProfileData {
        &self.profile_data
    }
}

/// States to use within a single partition.
///
/// Most operations will only have a single state (set to `current`) and an
/// empty `next` queue.
///
/// However for operations like Union where we're unioning multiple inputs, each
/// partition will be responsible for multiple states (for union, it'd be two
/// states, one for "top" and one for "bottom").
///
/// This lets us maintain the same number of partitions across the inputs and
/// the single output.
// TODO: Remove?
#[derive(Debug)]
struct PartitionStates {
    /// The current partition state being used for execution.
    current: PartitionState,
    /// Next states to use once we exhaust the operator using the current state.
    next: VecDeque<PartitionState>,
}

struct OperatorStackEffects<'a, 'b> {
    cx: &'a mut Context<'b>,
    operators: &'a mut [OperatorWithState],
}

impl<'a, 'b> StackEffectHandler for OperatorStackEffects<'a, 'b> {
    fn handle_execute(&mut self, op_idx: usize) -> Result<PollExecute> {
        // TODO: Use `get_many_mut` when that stabilizes.
        let (a, b) = self.operators.split_at_mut(op_idx);
        let child = a.last_mut(); // Allowed to be None.
        let operator = b.first_mut().expect("operator to exist");

        let inout = match child {
            Some(child) => ExecuteInOutState {
                input: child.output_buffer.as_mut(),
                output: operator.output_buffer.as_mut(),
            },
            None => ExecuteInOutState {
                input: None,
                output: operator.output_buffer.as_mut(),
            },
        };

        operator.physical.poll_execute(
            self.cx,
            &mut operator.partition_states.current,
            &operator.operator_state,
            inout,
        )
    }

    fn handle_finalize(&mut self, op_idx: usize) -> Result<PollFinalize> {
        self.operators[op_idx].physical.poll_finalize(
            self.cx,
            &mut self.operators[op_idx].partition_states.current,
            &self.operators[op_idx].operator_state,
        )
    }
}

impl ExecutablePartitionPipeline {
    /// Try to execute as much of the pipeline for this partition as possible.
    ///
    /// Loop through all operators, pushing data as far as we can until we get
    /// to a pending state, or we've completed the pipeline.
    ///
    /// When we reach a pending state, the state will be updated such that the
    /// next call to `poll_execute` will pick up where it left off.
    ///
    /// Once a batch has been pushed to the 'sink' operator (the last operator),
    /// the pull state gets reset such that this will begin pulling from the
    /// first non-exhausted operator.
    ///
    /// When an operator is exhausted (no more batches to pull), `poll_finalize`
    /// is called on the _next_ operator, and we begin pulling from the _next_
    /// operator until it's exhausted.
    ///
    /// The inner loop logic lives in `ExecutionStack`.
    pub fn poll_execute<I>(&mut self, cx: &mut Context) -> Poll<Option<Result<()>>>
    where
        I: RuntimeInstant,
    {
        trace!(
            pipeline_id = %self.info.pipeline.0,
            partition = %self.info.partition,
            "executing partition pipeline",
        );

        let mut handler = OperatorStackEffects {
            cx,
            operators: &mut self.operators,
        };

        loop {
            let control_flow = match self.stack.pop_next(&mut handler) {
                Ok(cf) => cf,
                Err(e) => return Poll::Ready(Some(Err(e))),
            };

            match control_flow {
                StackControlFlow::Continue => continue,
                StackControlFlow::Finished => return Poll::Ready(None),
                StackControlFlow::Pending => return Poll::Pending,
            }
        }
    }
}
