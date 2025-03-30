use std::sync::Arc;

use glaredb_core::runtime::pipeline::QueryHandle;
use glaredb_core::runtime::profile_buffer::ProfileBuffer;
use parking_lot::Mutex;

use super::task::{PartitionPipelineTask, TaskState};

/// Query handle for queries being executed on the threaded runtime.
#[derive(Debug)]
pub struct ThreadedQueryHandle {
    /// Registered task states for all pipelines in a query.
    pub(crate) states: Mutex<Vec<Arc<TaskState>>>,
    pub(crate) profiles: ProfileBuffer,
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

    fn get_profile_buffer(&self) -> &ProfileBuffer {
        &self.profiles
    }
}
