use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use parking_lot::Mutex;
use rayexec_error::RayexecError;
use rayexec_execution::execution::executable::partition_pipeline::ExecutablePartitionPipeline;
use rayexec_execution::runtime::ErrorSink;
use rayon::ThreadPool;

use crate::time::NativeInstant;

/// State shared by the partition pipeline task and the waker.
#[derive(Debug)]
pub(crate) struct TaskState {
    /// The partition pipeline we're operating on alongside a boolean for if the
    /// query's been canceled.
    pub(crate) pipeline: Mutex<PipelineState>,

    /// Error sink for any errors that occur during execution.
    pub(crate) errors: Arc<dyn ErrorSink>,

    /// The threadpool to execute on.
    pub(crate) pool: Arc<ThreadPool>,
}

#[derive(Debug)]
pub(crate) struct PipelineState {
    pub(crate) pipeline: ExecutablePartitionPipeline,
    pub(crate) query_canceled: bool,
}

/// Task for executing a partition pipeline.
pub struct PartitionPipelineTask {
    state: Arc<TaskState>,
}

impl PartitionPipelineTask {
    pub(crate) fn from_task_state(state: Arc<TaskState>) -> Self {
        PartitionPipelineTask { state }
    }

    pub(crate) fn execute(self) {
        let mut pipeline_state = self.state.pipeline.lock();

        if pipeline_state.query_canceled {
            self.state
                .errors
                .push_error(RayexecError::new("Query canceled"));
            return;
        }

        let waker: Waker = Arc::new(PartitionPipelineWaker {
            state: self.state.clone(),
        })
        .into();

        let mut cx = Context::from_waker(&waker);
        loop {
            match pipeline_state
                .pipeline
                .poll_execute::<NativeInstant>(&mut cx)
            {
                Poll::Ready(Some(Ok(()))) => {
                    // Pushing through the pipeline was successful. Continue the
                    // loop to try to get as much work done as possible.
                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    self.state.errors.push_error(e);
                    return;
                }
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

/// A waker implementation that will re-execute the pipeline once woken.
struct PartitionPipelineWaker {
    state: Arc<TaskState>,
}

impl Wake for PartitionPipelineWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let pool = self.state.pool.clone();
        let task = PartitionPipelineTask {
            state: self.state.clone(),
        };
        pool.spawn(|| task.execute());
    }
}
