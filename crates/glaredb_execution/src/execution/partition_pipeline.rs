use std::task::{Context, Poll};

use glaredb_error::Result;

use super::execution_stack::{Effects, ExecutionStack};
use super::operators::{
    AnyOperatorState,
    AnyPartitionState,
    PlannedOperator,
    PollExecute,
    PollFinalize,
};
use crate::arrays::batch::Batch;
use crate::catalog::profile::PartitionPipelineProfile;
use crate::execution::execution_stack::StackControlFlow;
use crate::runtime::time::RuntimeInstant;

#[derive(Debug)]
pub struct ExecutablePartitionPipeline {
    /// All operators in this pipeline.
    ///
    /// The first operator is the "source" operator, while the last is the
    /// "sink" operator.
    pub(crate) operators: Vec<PlannedOperator>,
    /// States for each operator. Shared across all partitions.
    pub(crate) operator_states: Vec<AnyOperatorState>,
    /// Partition states for each operator.
    pub(crate) partition_states: Vec<AnyPartitionState>,
    /// Batch buffers for storing intermediate results.
    ///
    /// The 'i'th batch corresponds to the output of the 'i'th operator.
    ///
    /// This will be one less than the total number of operators since the
    /// "sink" produces no output for this pipeline.
    pub(crate) buffers: Vec<Batch>,
    /// Controls the execution of the operators for this partition.
    pub(crate) stack: ExecutionStack,
    /// Execution profile for this pipeline.
    pub(crate) profile: PartitionPipelineProfile,
}

impl ExecutablePartitionPipeline {
    /// Try to execute as much of the pipeline for this partition as possible.
    ///
    /// Loop through all operators, pushing data as far as we can until we get
    /// to a pending state, or we've completed the pipeline.
    ///
    /// Once a batch has been pushed to the 'sink' operator (the last operator),
    /// the pull state gets reset such that this will begin pulling from the
    /// first non-exhausted operator.
    ///
    /// When an operator is exhausted (no more batches to pull), `poll_finalize`
    /// is called on the _next_ operator, and we begin pulling from the _next_
    /// operator until it's exhausted.
    ///
    /// This will attempt to execute as much of the pipeline as possible. A
    /// return value of `Poll::Ready(Ok(()))` indicates that this partition
    /// pipeline is complete and everything's been written to the sink.
    ///
    /// When we reach a pending state, the state will be updated such that the
    /// next call to `poll_execute` will pick up where it left off.
    ///
    /// The inner logic lives in `ExecutionStack`.
    pub fn poll_execute<I>(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        I: RuntimeInstant,
    {
        let mut effects = OperatorEffects {
            cx,
            operators: &self.operators,
            operator_states: &self.operator_states,
            partition_states: &mut self.partition_states,
            buffers: &mut self.buffers,
        };

        loop {
            let control_flow = match self.stack.pop_next(&mut effects) {
                Ok(cf) => cf,
                Err(e) => return Poll::Ready(Err(e)),
            };

            match control_flow {
                StackControlFlow::Continue => continue,
                StackControlFlow::Finished => return Poll::Ready(Ok(())),
                StackControlFlow::Pending => return Poll::Pending,
            }
        }
    }

    pub fn profile(&self) -> &PartitionPipelineProfile {
        // TODO: We should probably just return the profile when we finish
        // execute.
        &self.profile
    }
}

struct OperatorEffects<'a, 'b> {
    cx: &'a mut Context<'b>,
    operators: &'a [PlannedOperator],
    operator_states: &'a [AnyOperatorState],
    partition_states: &'a mut [AnyPartitionState],
    buffers: &'a mut [Batch],
}

impl Effects for OperatorEffects<'_, '_> {
    fn handle_execute(&mut self, op_idx: usize) -> Result<PollExecute> {
        if op_idx == 0 {
            // Pulling from pipeline source.
            let poll = self.operators[0].call_poll_pull(
                self.cx,
                &self.operator_states[0],
                &mut self.partition_states[0],
                &mut self.buffers[0],
            )?;

            return Ok(poll.as_poll_execute());
        }

        if op_idx == self.operators.len() - 1 {
            // Pushing to pipeline sink. Get the last batch from the operator
            // completely owned by our pipeline, and use that as the input.
            let pipeline_output = &mut self.buffers[op_idx - 1];
            let poll = self.operators[op_idx].call_poll_push(
                self.cx,
                &self.operator_states[op_idx],
                &mut self.partition_states[op_idx],
                pipeline_output,
            )?;

            return Ok(poll.as_poll_execute());
        }

        // Otherwise just an intermediate operator in the pipeline. Use the
        // previous operator's output batch as this operator's input.
        let (input, output) = get_execute_inout(op_idx, self.buffers);

        self.operators[op_idx].call_poll_execute(
            self.cx,
            &self.operator_states[op_idx],
            &mut self.partition_states[op_idx],
            input,
            output,
        )
    }

    fn handle_finalize(&mut self, op_idx: usize) -> Result<PollFinalize> {
        assert_ne!(0, op_idx);

        if op_idx == self.operators.len() - 1 {
            // Finalizing pushing to the "sink".
            let poll = self.operators[op_idx].call_poll_finalize_push(
                self.cx,
                &self.operator_states[op_idx],
                &mut self.partition_states[op_idx],
            )?;

            // TODO: Should we check that the result is sane? E.g. NeedsDrain
            // wouldn't make sense on the push side, just the execute side.
            return Ok(poll);
        }

        // Normal execute finalize.
        self.operators[op_idx].call_poll_finalize_execute(
            self.cx,
            &self.operator_states[op_idx],
            &mut self.partition_states[op_idx],
        )
    }
}

fn get_execute_inout(op_idx: usize, batches: &mut [Batch]) -> (&mut Batch, &mut Batch) {
    assert!(op_idx != 0);
    assert!(op_idx < batches.len());

    let child_idx = op_idx - 1;

    // TODO: Replace with `get_many_mut` when stabilized.
    let (before, after) = batches.split_at_mut(op_idx);
    let child = &mut before[child_idx];
    let op = &mut after[0]; // `op_idx` is the first element in `after`

    (child, op)
}
