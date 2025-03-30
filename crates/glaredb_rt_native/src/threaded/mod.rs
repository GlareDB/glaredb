mod handle;
mod task;

use std::fmt;
use std::sync::Arc;

use glaredb_core::execution::partition_pipeline::ExecutablePartitionPipeline;
use glaredb_core::runtime::pipeline::ErrorSink;
use glaredb_core::runtime::profile_buffer::ProfileBuffer;
use glaredb_error::{DbError, Result};
use handle::ThreadedQueryHandle;
use parking_lot::Mutex;
use rayon::{ThreadPool, ThreadPoolBuilder};
use task::{PartitionPipelineTask, PipelineState, TaskState};
use tracing::debug;

use crate::runtime::Scheduler;

/// Work-stealing scheduler for executing queries on a thread pool.
#[derive(Clone)]
pub struct ThreadedScheduler {
    pool: Arc<ThreadPool>,
}

impl fmt::Debug for ThreadedScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("num_threads", &self.pool.current_num_threads())
            .finish_non_exhaustive()
    }
}

impl Scheduler for ThreadedScheduler {
    type Handle = ThreadedQueryHandle;

    fn try_new(num_threads: usize) -> Result<Self> {
        let thread_pool = ThreadPoolBuilder::new()
            .thread_name(|idx| format!("rayexec_compute_{idx}"))
            .num_threads(num_threads)
            .build()
            .map_err(|e| DbError::with_source("Failed to build thread pool", Box::new(e)))?;

        Ok(ThreadedScheduler {
            pool: Arc::new(thread_pool),
        })
    }

    fn num_threads(&self) -> usize {
        self.pool.current_num_threads()
    }

    /// Spawn execution of a query graph on the thread pool.
    ///
    /// Each partition pipeline in the query graph will be independently
    /// executed.
    fn spawn_pipelines(
        &self,
        pipelines: Vec<ExecutablePartitionPipeline>,
        errors: Arc<dyn ErrorSink>,
    ) -> ThreadedQueryHandle {
        debug!("spawning execution of query graph");

        let (profiles, profile_sinks) = ProfileBuffer::new(pipelines.len());

        let task_states: Vec<_> = pipelines
            .into_iter()
            .zip(profile_sinks)
            .map(|(pipeline, profile_sink)| {
                Arc::new(TaskState {
                    pipeline: Mutex::new(PipelineState {
                        pipeline,
                        query_canceled: false,
                    }),
                    errors: errors.clone(),
                    pool: self.pool.clone(),
                    profile_sink,
                })
            })
            .collect();

        let handle = ThreadedQueryHandle {
            states: Mutex::new(task_states.clone()),
            profiles,
        };

        for state in task_states {
            let task = PartitionPipelineTask::from_task_state(state);
            self.pool.spawn(|| task.execute());
        }

        handle
    }
}
