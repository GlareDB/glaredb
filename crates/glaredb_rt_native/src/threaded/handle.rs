use std::sync::Arc;

use futures::future::BoxFuture;
use parking_lot::Mutex;
use glaredb_error::{not_implemented, Result};
use glaredb_execution::runtime::handle::{ExecutionProfileData, QueryHandle};

use super::task::{PartitionPipelineTask, TaskState};

/// Query handle for queries being executed on the threaded runtime.
#[derive(Debug)]
pub struct ThreadedQueryHandle {
    /// Registered task states for all pipelines in a query.
    pub(crate) states: Mutex<Vec<Arc<TaskState>>>,
}

impl QueryHandle for ThreadedQueryHandle {
    /// Cancel the query.
    fn cancel(&self) {
        let states = self.states.lock();

        for state in states.iter() {
            let mut pipeline = state.pipeline.lock();
            pipeline.query_canceled = true;
            std::mem::drop(pipeline);

            // Re-execute the pipeline so it picks up the set bool. This lets us
            // cancel the pipeline regardless of if it's pending.
            let task = PartitionPipelineTask::from_task_state(state.clone());
            task.execute()
        }
    }

    fn generate_execution_profile_data(&self) -> BoxFuture<'_, Result<ExecutionProfileData>> {
        Box::pin(async { not_implemented!("generate execution profile data") })
    }
}
