use rayexec_error::{Result};
use rayon::ThreadPool;
use std::{
    fmt,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};
use tracing::trace;

use super::{chain::OperatorChain, Pipeline};

/// Scheduler for executing query pipelines on a rayon thread pool.
pub struct Scheduler {
    sync_pool: Arc<ThreadPool>,
    // TODO: At some point including an "io thread pool" (tokio) for async io
    // stuff if needed.
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("num_threads", &self.sync_pool.current_num_threads())
            .finish_non_exhaustive()
    }
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
        trace!(?pipeline, "executing pipeline");

        let pipeline = Arc::new(pipeline);

        for (chain_idx, chain) in pipeline.chains.iter().enumerate() {
            for partition in 0..chain.partitions() {
                let waker = Arc::new(OperatorPartitionWaker {
                    pipeline: pipeline.clone(),
                    operator: chain_idx,
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
        let chain = self.waker.get_own_chain();

        let waker: Waker = self.waker.clone().into();
        let mut cx = Context::from_waker(&waker);

        loop {
            match chain.poll_execute(&mut cx, self.waker.partition) {
                Poll::Ready(Some(Ok(()))) => {
                    // Pushing through the operator chain was successful.
                    // Continue the loop to try to get as much work done as
                    // possible.
                    continue;
                }
                Poll::Ready(Some(Err(e))) => panic!("Schedule error: {e}"),
                Poll::Pending => {
                    // Exit the loop. Waker was already stored in the pending
                    // sink/source, we'll be woken back up when there's more
                    // this operator chain can start executing.
                    return;
                }
                Poll::Ready(None) => {
                    // Exit the loop, nothing else for us to do. Waker is not
                    // stored, and we will not executed again.
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

    /// Index of the operator chain this worker is executing for.
    operator: usize,

    /// Partition of the pipeline this worker is executing for.
    partition: usize,

    /// Thread pool we'll be spawning work on.
    sync_pool: Arc<ThreadPool>,
}

impl OperatorPartitionWaker {
    /// Get the operator chanins that this waker is working on.
    fn get_own_chain(&self) -> &OperatorChain {
        &self.pipeline.chains[self.operator]
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
