use crate::types::batch::DataBatch;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use std::{
    fmt::Debug,
    task::{Context, Poll},
};

use super::plans::{PhysicalOperator, Sink, Source};

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
    pub fn poll_execute(&self, cx: &mut Context, partition: usize) -> Poll<Option<Result<()>>> {
        let mut state = self.states[partition].lock();
        loop {
            match &mut *state {
                PartitionState::Pull => match self.source.poll_next(cx, partition) {
                    Poll::Ready(Some(Ok(batch))) => {
                        // We got a batch, run it through the executors.
                        match self.execute_operators(batch) {
                            Ok(batch) => {
                                // Now try to push to sink.
                                *state = PartitionState::PushPending { batch: Some(batch) };
                                continue;
                            }
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        // This partition is exhausted for the source. Mark as
                        // finished.
                        *state = PartitionState::Finished;
                        continue;
                    }
                    Poll::Pending => {
                        // Setting state here not needed since it's already in Pull,
                        // but just to be explicit.
                        *state = PartitionState::Pull;
                        return Poll::Pending;
                    }
                },
                PartitionState::PushPending { batch } => {
                    // Try to push again.
                    match self.sink.poll_ready(cx, partition) {
                        Poll::Ready(_) => {
                            // We're good to push.
                            match self
                                .sink
                                .push(batch.take().expect("batch to exist"), partition)
                            {
                                Ok(_) => {
                                    // Successfully pushed, reset state back to Pull
                                    // to get the next batch.
                                    *state = PartitionState::Pull;
                                    continue;
                                }
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            }
                        }
                        Poll::Pending => {
                            // Still need to wait before we can push.
                            *state = PartitionState::PushPending {
                                batch: batch.take(),
                            };
                            return Poll::Pending;
                        }
                    }
                }
                PartitionState::Finished => match self.sink.finish(partition) {
                    Ok(_) => return Poll::Ready(None),
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
            }
        }
    }

    fn execute_operators(&self, mut batch: DataBatch) -> Result<DataBatch> {
        for operator in &self.operators {
            batch = operator.execute(batch)?;
        }
        Ok(batch)
    }
}
