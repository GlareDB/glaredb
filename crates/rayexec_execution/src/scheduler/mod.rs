pub mod future;
pub mod query;

use rayon::{ThreadPool, ThreadPoolBuilder};
use std::future::Future;
use std::sync::Arc;
use std::{fmt, sync::mpsc};
use tracing::debug;

use crate::execution::pipeline::PartitionPipeline;
use crate::execution::query_graph::QueryGraph;
use rayexec_error::{RayexecError, Result};

use self::future::FutureTask;
use self::query::PartitionPipelineTask;

/// Scheduler for executing queries and other tasks.
#[derive(Clone)]
pub struct ComputeScheduler {
    pool: Arc<ThreadPool>,
}

impl fmt::Debug for ComputeScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("num_threads", &self.pool.current_num_threads())
            .finish_non_exhaustive()
    }
}

impl ComputeScheduler {
    pub fn try_new() -> Result<Self> {
        let thread_pool = ThreadPoolBuilder::new()
            .thread_name(|idx| format!("rayexec_compute_{idx}"))
            .build()
            .map_err(|e| RayexecError::with_source("Failed to build thread pool", Box::new(e)))?;

        Ok(ComputeScheduler {
            pool: Arc::new(thread_pool),
        })
    }

    /// Spawn execution of a partition pipeline on the thread pool.
    pub fn spawn_partition_pipeline(
        &self,
        pipeline: PartitionPipeline,
        errors: mpsc::Sender<RayexecError>,
    ) {
        let task = PartitionPipelineTask::new(pipeline, errors);
        let pool = self.pool.clone();
        self.pool.spawn(|| task.execute(pool));
    }

    /// Spawn execution of a query graph on the thread pool.
    ///
    /// Each partition pipeline in the query graph will be independently
    /// executed.
    pub fn spawn_query_graph(&self, query_graph: QueryGraph, errors: mpsc::Sender<RayexecError>) {
        debug!("spawning execution of query graph");
        for partition_pipeline in query_graph.into_partition_pipeline_iter() {
            self.spawn_partition_pipeline(partition_pipeline, errors.clone());
        }
    }

    /// Spawn a future on the scheduler.
    ///
    /// The future will immediately start executing.
    // TODO: Return handle for cancel/result.
    pub fn spawn_future<F>(&self, fut: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let task = Arc::new(FutureTask::new(Box::pin(fut)));
        let pool = self.pool.clone();
        self.pool.spawn(|| task.execute(pool));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[test]
    fn spawn_basic_future() {
        let completed = Arc::new(AtomicBool::new(false));
        let completed_c = completed.clone();
        let fut = async move {
            // Do some work ...
            completed_c.store(true, Ordering::SeqCst);
        };
        let task = Arc::new(FutureTask::new(Box::pin(fut)));

        let scheduler = ComputeScheduler::try_new().unwrap();
        task.execute(scheduler.pool.clone());

        assert!(completed.load(Ordering::SeqCst));
    }
}
