use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use glaredb_core::execution::partition_pipeline::ExecutablePartitionPipeline;
use glaredb_core::runtime::pipeline::ErrorSink;
use glaredb_core::runtime::profile_buffer::ProfileSink;
use glaredb_error::DbError;
use parking_lot::Mutex;
use rayon::ThreadPool;
use tracing::warn;

use crate::time::NativeInstant;

#[derive(Debug)]
pub(crate) struct PipelineState {
    pub(crate) pipeline: ExecutablePartitionPipeline,
    pub(crate) query_canceled: bool,
    /// If the pipeline is finished.
    // TODO: This should be removed, and instead ensure we schedule execution
    // exactly once per waker.
    pub(crate) finished: bool,
}

#[derive(Debug)]
pub(crate) struct TaskState {
    /// The partition pipeline we're operating on alongside a boolean for if the
    /// query's been canceled.
    pub(crate) pipeline: Mutex<PipelineState>,
    /// Error sink for any errors that occur during execution.
    pub(crate) errors: Arc<dyn ErrorSink>,
    /// The threadpool to execute on.
    pub(crate) pool: Arc<ThreadPool>,
    /// Where to put the profile when this pipeline completes.
    pub(crate) profile_sink: ProfileSink,
}

impl Wake for TaskState {
    fn wake(self: Arc<Self>) {
        let pool = self.pool.clone();
        pool.spawn(|| self.execute());
    }

    fn wake_by_ref(self: &Arc<Self>) {
        Arc::clone(self).wake();
    }
}

impl TaskState {
    pub(crate) fn execute(self: Arc<Self>) {
        let mut pipeline_state = self.pipeline.lock();

        if pipeline_state.query_canceled {
            self.errors.set_error(DbError::new("Query canceled"));
            return;
        }

        // TODO: This a temporary measure to handle spurious wakeups. It's fine
        // for now, but we should instead ensure a waker will be scheduled for
        // execution exactly once.
        if pipeline_state.finished {
            warn!("Attempted to execute task that's finished");
            return;
        }

        let waker: Waker = self.clone().into();
        let mut cx = Context::from_waker(&waker);

        match pipeline_state
            .pipeline
            .poll_execute::<NativeInstant>(&mut cx)
        {
            Poll::Ready(Ok(prof)) => {
                // Pushing through the pipeline was successful. Put our profile.
                // We'll never execute again.
                self.profile_sink.put(prof);
                pipeline_state.finished = true;
            }
            Poll::Ready(Err(e)) => {
                self.errors.set_error(e);
            }
            Poll::Pending => {
                // Exit the loop. Waker was already stored in the pending
                // sink/source, we'll be woken back up when there's more
                // this operator chain can start executing.
            }
        }
    }
}
