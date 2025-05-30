use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use glaredb_core::execution::partition_pipeline::ExecutablePartitionPipeline;
use glaredb_core::runtime::pipeline::ErrorSink;
use glaredb_core::runtime::profile_buffer::ProfileSink;
use glaredb_error::DbError;
use parking_lot::Mutex;
use rayon::ThreadPool;

use crate::time::NativeInstant;

const SCHEDULED: u8 = 0b01;
const PENDING: u8 = 0b10;

#[derive(Debug)]
pub(crate) struct PipelineState {
    pub(crate) pipeline: ExecutablePartitionPipeline,
    pub(crate) query_canceled: bool,
}

#[derive(Debug)]
pub(crate) struct ScheduleState {
    pub(crate) running: bool,
    pub(crate) pending: bool,
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
    pub(crate) sched_state: Mutex<ScheduleState>,
}

impl Wake for TaskState {
    fn wake(self: Arc<Self>) {
        self.schedule();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        Arc::clone(self).wake();
    }
}

impl TaskState {
    pub(crate) fn schedule(self: Arc<Self>) {
        let mut sched_guard = self.sched_state.lock();
        if sched_guard.running {
            sched_guard.pending = true;
        } else {
            sched_guard.running = true;
            std::mem::drop(sched_guard);

            let state = self.clone();
            self.pool.spawn(move || {
                loop {
                    state.execute();

                    let mut sched_guard = state.sched_state.lock();
                    if sched_guard.pending {
                        sched_guard.pending = false;
                        // Continue...
                    } else {
                        // Nom more work.
                        sched_guard.running = false;
                        break;
                    }
                }
            });
        }
    }

    pub(crate) fn execute(self: &Arc<Self>) {
        let mut pipeline_state = self.pipeline.lock();

        if pipeline_state.query_canceled {
            self.errors.set_error(DbError::new("Query canceled"));
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
