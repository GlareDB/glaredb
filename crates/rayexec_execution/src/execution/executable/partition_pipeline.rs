use std::sync::Arc;
use std::task::{Context, Poll};

use rayexec_error::Result;
use tracing::trace;

use super::pipeline::PipelineId;
use super::stack::{ExecutionStack, StackEffectHandler};
use crate::arrays::batch::Batch;
use crate::execution::executable::stack::StackControlFlow;
use crate::execution::operators::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PhysicalOperator,
    PollExecute,
    PollFinalize,
};
use crate::runtime::time::RuntimeInstant;

/// Information about a partition pipeline.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PartitionPipelineInfo {
    pub pipeline: PipelineId,
    pub partition: usize,
}

/// Executable pipeline for a single partition.
#[derive(Debug)]
pub struct ExecutablePartitionPipeline {
    pub(crate) info: PartitionPipelineInfo,
    pub(crate) stack: ExecutionStack,
    pub(crate) operators: Vec<OperatorWithState>,
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
    /// The inner logic lives in `ExecutionStack`.
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

        // TODO: Need to either remove this loop or the loop in the executors.
        loop {
            let control_flow = match self.stack.pop_next(&mut handler) {
                Ok(cf) => cf,
                Err(e) => return Poll::Ready(Some(Err(e))),
            };

            match control_flow {
                StackControlFlow::Continue => continue,
                StackControlFlow::Finished => {
                    // Current stream finished, move to the next stream if its
                    // available.
                    // if self.current_stream
                }
                StackControlFlow::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Debug)]
pub struct OperatorWithState {
    /// The underlying physical operator.
    physical: Arc<PhysicalOperator>,
    /// The state that's shared across all partitions for this operator.
    operator_state: Arc<OperatorState>,
    /// State for this operator that's exclusive to this partition.
    partition_states: PartitionState,
    /// Output batch buffer for this operator/pipeline.
    ///
    /// May be None if this operator is the last in the pipeline.
    output_buffer: Option<Batch>,
}

/// Implementation of `StackEffectsHandler` that calls `poll_finalize` and
/// `poll_execute` on the appropriate operators.
struct OperatorStackEffects<'a, 'b> {
    cx: &'a mut Context<'b>,
    operators: &'a mut [OperatorWithState],
}

impl<'a, 'b> StackEffectHandler for OperatorStackEffects<'a, 'b> {
    fn handle_execute(&mut self, op_idx: usize) -> Result<PollExecute> {
        let (operator, child) = get_operator_and_child_mut(op_idx, self.operators);

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
            &mut operator.partition_states,
            &operator.operator_state,
            inout,
        )
    }

    fn handle_finalize(&mut self, op_idx: usize) -> Result<PollFinalize> {
        let operator = &mut self.operators[op_idx];
        operator.physical.poll_finalize(
            self.cx,
            &mut operator.partition_states,
            &operator.operator_state,
        )
    }
}

/// Get an operator and its child if it has one.
///
/// Operators at the start of a stream do not have child (e.g. table scans).
fn get_operator_and_child_mut(
    op_idx: usize,
    operators: &mut [OperatorWithState],
) -> (&mut OperatorWithState, Option<&mut OperatorWithState>) {
    if op_idx == 0 {
        let op = &mut operators[0];
        return (op, None);
    }

    let child_idx = op_idx - 1;

    assert!(op_idx < operators.len());

    let ptr = operators.as_mut_ptr();
    // SAFETY: We asserted that indices are unique and in bounds.
    // TODO: Replace with `get_many_mut` when stabilized.
    unsafe {
        let op = &mut *ptr.add(op_idx);
        let child = &mut *ptr.add(child_idx);
        (op, Some(child))
    }
}
