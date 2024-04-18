use crate::types::batch::DataBatch;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use std::{
    fmt::Debug,
    task::{Context, Poll},
};

use super::{
    plans::{PhysicalOperator, PollPull, PollPush, Sink, Source},
    TaskContext,
};

/// An operator chain represents a subset of computation in the execution
/// pipeline.
///
/// During execution, batches are pulled from a source, ran through a sequence
/// of stateless operators, then pushed to a sink.
#[derive(Debug)]
pub struct OperatorChain {
    /// Sink where all output batches are pushed to.
    sink: Box<dyn Sink>,

    /// Sequence of operators to run batches through. Executed left to right.
    operators: Vec<Box<dyn PhysicalOperator>>,

    /// Where to pull originating batches.
    source: Box<dyn Source>,

    /// Partition-local states.
    states: Vec<Mutex<PartitionState>>,
}

impl OperatorChain {
    pub fn sink(&self) -> &dyn Sink {
        self.sink.as_ref()
    }

    pub fn operators(&self) -> &[Box<dyn PhysicalOperator>] {
        self.operators.as_slice()
    }

    pub fn source(&self) -> &dyn Source {
        self.source.as_ref()
    }
}

/// State local to each partition in the chain.
#[derive(Debug)]
enum PartitionState {
    /// Need to pull from the source.
    Pull,

    /// Sink not yet ready. Store the intermediate batch.
    // TODO: Remove Option. Added in to make borrow checker happy below.
    PushPending { batch: Option<DataBatch> },

    /// All batches successfully pushed, we're done.
    Finished,
}

impl OperatorChain {
    /// Create a new operator chain.
    ///
    /// Errors if the the number of output partitions from the source differs
    /// from the number of input partitions for the sink.
    pub fn try_new(
        source: Box<dyn Source>,
        sink: Box<dyn Sink>,
        operators: Vec<Box<dyn PhysicalOperator>>,
    ) -> Result<Self> {
        if source.output_partitions() != sink.input_partitions() {
            return Err(RayexecError::new(format!(
                "Different partition counts for source and sink. Source: {}, Sink: {}",
                source.output_partitions(),
                sink.input_partitions()
            )));
        }

        let partitions = source.output_partitions();
        let states = (0..partitions)
            .map(|_| Mutex::new(PartitionState::Pull))
            .collect();

        Ok(OperatorChain {
            sink,
            operators,
            source,
            states,
        })
    }

    /// Return the number of partitions that this pipeline should be executing.
    pub fn partitions(&self) -> usize {
        self.sink.input_partitions()
    }

    /// Execute the operator chain for a partition.
    pub fn poll_execute(
        &self,
        task_cx: &TaskContext,
        cx: &mut Context,
        partition: usize,
    ) -> Poll<Option<Result<()>>> {
        let mut state = self.states[partition].lock();
        loop {
            match &mut *state {
                PartitionState::Pull => match self.source.poll_pull(task_cx, cx, partition) {
                    Ok(PollPull::Batch(batch)) => {
                        // We got a batch, run it through the executors.
                        match self.execute_operators(task_cx, batch) {
                            Ok(batch) => {
                                // Now try to push to sink.
                                *state = PartitionState::PushPending { batch: Some(batch) };
                                continue;
                            }
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                    Ok(PollPull::Pending) => {
                        // Setting state here not needed since it's already in Pull,
                        // but just to be explicit.
                        *state = PartitionState::Pull;
                        return Poll::Pending;
                    }
                    Ok(PollPull::Exhausted) => {
                        // This partition is exhausted for the source. Mark as
                        // finished.
                        *state = PartitionState::Finished;
                        continue;
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
                PartitionState::PushPending { batch } => {
                    // Batch should always exist at this point. It's currently
                    // wrapped in an Option to satisfy the borrow checker here
                    // (we have a mut reference, not the owned reference).
                    let batch = batch.take().expect("batch to exist");

                    match self.sink.poll_push(task_cx, cx, batch, partition) {
                        Ok(PollPush::Pushed) => {
                            // Successfully pushed, reset state back to Pull
                            // to get the next batch.
                            *state = PartitionState::Pull;
                            continue;
                        }
                        Ok(PollPush::Pending(batch)) => {
                            // Still need to wait before we can push.
                            *state = PartitionState::PushPending { batch: Some(batch) };
                            return Poll::Pending;
                        }
                        Ok(PollPush::Break) => {
                            // This sink requires no more input.
                            *state = PartitionState::Finished;
                            continue;
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                PartitionState::Finished => match self.sink.finish(task_cx, partition) {
                    Ok(_) => return Poll::Ready(None),
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
            }
        }
    }

    fn execute_operators(&self, task_cx: &TaskContext, mut batch: DataBatch) -> Result<DataBatch> {
        for operator in &self.operators {
            batch = operator.execute(task_cx, batch)?;
        }
        Ok(batch)
    }
}
