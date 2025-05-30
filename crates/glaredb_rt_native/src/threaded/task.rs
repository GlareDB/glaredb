use std::sync::Arc;
use std::sync::atomic::{self, AtomicBool};
use std::task::{Context, Poll, Wake, Waker};

use glaredb_core::execution::partition_pipeline::ExecutablePartitionPipeline;
use glaredb_core::runtime::pipeline::ErrorSink;
use glaredb_core::runtime::profile_buffer::ProfileSink;
use glaredb_error::DbError;
use parking_lot::Mutex;
use rayon::ThreadPool;

use crate::time::NativeInstant;

#[derive(Debug)]
pub(crate) struct PipelineState {
    pub(crate) pipeline: ExecutablePartitionPipeline,
    pub(crate) query_canceled: bool,
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
    pub(crate) scheduled: AtomicBool,
    pub(crate) pending_wake: AtomicBool,
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
        // If we were not already scheduled, go ahead and spwn now.
        if !self.scheduled.swap(true, atomic::Ordering::AcqRel) {
            let state = self.clone();
            self.pool.spawn(move || {
                // As long as there's a pending wake, keep polling.
                loop {
                    // Clear pending.
                    state.pending_wake.store(false, atomic::Ordering::Release);

                    // Execute! (locks internally)
                    state.clone().execute();

                    // If nobody woke us in the meantime, we're done.
                    if !state.pending_wake.swap(false, atomic::Ordering::AcqRel) {
                        break;
                    }
                    // Otherwise there was at least one wake. Continue...
                    // execute again.
                }
                // Set no tasks inflight.
                state.scheduled.store(false, atomic::Ordering::Release);

                // We may have had a wakeup between setting `pending_wake` to
                // false and setting `scheduled` to false.
                //
                // Recheck and reschedule if that happened.
                if state.pending_wake.swap(false, atomic::Ordering::AcqRel) {
                    state.schedule();
                }
            });
        } else {
            // If we are already scheduled, just record that a wake happened.
            self.pending_wake.store(true, atomic::Ordering::Release);
        }
    }

    pub(crate) fn execute(self: Arc<Self>) {
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
