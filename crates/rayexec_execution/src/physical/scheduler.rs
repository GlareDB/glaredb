use crate::errors::{err, Result};
use rayon::ThreadPool;
use std::{
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use super::{Destination, LinkedOperator, Pipeline};

/// Scheduler for executing query pipelines on a rayon thread pool.
pub struct Scheduler {
    sync_pool: Arc<ThreadPool>,
    // TODO: At some point including an "io thread pool" (tokio) for async io
    // stuff if needed.
}

impl Scheduler {
    pub fn new(sync_pool: Arc<ThreadPool>) -> Scheduler {
        Scheduler { sync_pool }
    }

    /// Executes the pipeline on the thread pool.
    ///
    /// Under the hood, this will create independent tasks for each (operator,
    /// partition) pair which will all immediately start executing. However,
    /// only some will make progress while the others will be woken up once
    /// there's batches to begin executing on.
    pub fn execute(&self, pipeline: Pipeline) -> Result<()> {
        let pipeline = Arc::new(pipeline);

        for (operator_idx, operator) in pipeline.operators.iter().enumerate() {
            for partition in 0..operator.operator.output_partitions() {
                let waker = Arc::new(OperatorPartitionWaker {
                    pipeline: pipeline.clone(),
                    operator: operator_idx,
                    partition,
                    sync_pool: self.sync_pool.clone(),
                });

                let task = OperatorPartitionTask { waker };

                self.sync_pool.spawn(|| task.execute());
            }
        }

        Ok(())
    }
}

/// A task for executing a part of the pipeline.
struct OperatorPartitionTask {
    /// Waker which happens to have all the info we need to execute an operator.
    waker: Arc<OperatorPartitionWaker>,
}

impl OperatorPartitionTask {
    /// Execute this task.
    fn execute(self) {
        let linked = self.waker.get_own_operator();

        let waker: Waker = self.waker.clone().into();
        let mut cx = Context::from_waker(&waker);

        loop {
            match linked
                .operator
                .poll_partition(&mut cx, self.waker.partition)
            {
                Poll::Ready(Some(Ok(batch))) => {
                    // Partition for this operator produced a batch, send to the
                    // destination operator.
                    match linked.dest {
                        Destination::Operator { operator, child } => {
                            self.waker.pipeline.operators[operator]
                                .operator
                                .push(batch, child, self.waker.partition)
                                .unwrap(); // TODO
                        }
                        Destination::PipelineOutput => {
                            self.waker
                                .pipeline
                                .destination
                                .push(batch, 0, self.waker.partition)
                                .unwrap() // TODO
                        }
                    }

                    // We'll be continuing the loop to get as much work done as
                    // possible.
                }
                Poll::Ready(Some(Err(e))) => {
                    panic!("{e}")
                }
                Poll::Ready(None) => {
                    // Partition for this operator is done.
                    match linked.dest {
                        Destination::Operator { operator, child } => {
                            self.waker.pipeline.operators[operator]
                                .operator
                                .finish(child, self.waker.partition)
                                .unwrap(); // TODO
                        }
                        Destination::PipelineOutput => {
                            self.waker
                                .pipeline
                                .destination
                                .finish(0, self.waker.partition)
                                .unwrap() // TODO
                        }
                    }

                    // Exit the loop, nothing else for us to do. Waker is not
                    // stored, and we will not executed again.
                    return;
                }
                Poll::Pending => {
                    // Exit the loop. Waker was already stored, we'll be woken
                    // back up when there's a batch available for this partition
                    // in this operator.
                    return;
                }
            }
        }
    }
}

/// A waker responsible for spawning a task for working on an operator's
/// partition.
///
/// There should only exist a single waker per partition per pipeline.
struct OperatorPartitionWaker {
    /// The pipeline we're working on.
    pipeline: Arc<Pipeline>,

    /// Index of the operator this worker is executing for.
    operator: usize,

    /// Partition of the pipeline this worker is executing for.
    partition: usize,

    /// Thread pool we'll be spawning work on.
    sync_pool: Arc<ThreadPool>,
}

impl OperatorPartitionWaker {
    /// Get the operator that this waker is working on.
    fn get_own_operator(&self) -> &LinkedOperator {
        &self.pipeline.operators[self.operator]
    }
}

impl Wake for OperatorPartitionWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let task = OperatorPartitionTask {
            waker: self.clone(),
        };
        self.sync_pool.spawn(|| task.execute())
    }
}
