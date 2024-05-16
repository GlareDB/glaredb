pub mod future;
pub mod query;

use rayon::{ThreadPool, ThreadPoolBuilder};
use std::fmt;
use std::sync::Arc;
use tracing::info;

use crate::execution::pipeline::PartitionPipeline;
use crate::execution::query_graph::QueryGraph;
use rayexec_error::{RayexecError, Result};

use self::query::PartitionPipelineTask;

/// Scheduler for executing queries and other tasks.
#[derive(Clone)]
pub struct Scheduler {
    pool: Arc<ThreadPool>,
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("num_threads", &self.pool.current_num_threads())
            .finish_non_exhaustive()
    }
}

impl Scheduler {
    pub fn try_new() -> Result<Self> {
        let thread_pool = ThreadPoolBuilder::new()
            .thread_name(|idx| format!("glaredb-thread-{idx}"))
            .build()
            .map_err(|e| RayexecError::with_source("Failed to build thread pool", Box::new(e)))?;

        Ok(Scheduler {
            pool: Arc::new(thread_pool),
        })
    }

    /// Spawn execution of a partition pipeline on the thread pool.
    pub fn spawn_partition_pipeline(&self, pipeline: PartitionPipeline) {
        let task = PartitionPipelineTask::new(pipeline);
        let pool = self.pool.clone();
        self.pool.spawn(|| task.execute(pool));
    }

    /// Spawn execution of a query graph on the thread pool.
    ///
    /// Each partition pipeline in the query graph will be independently
    /// executed.
    pub fn spawn_query_graph(&self, query_graph: QueryGraph) {
        info!("spawning execution of query graph");
        for partition_pipeline in query_graph.into_partition_pipeline_iter() {
            self.spawn_partition_pipeline(partition_pipeline);
        }
    }
}
