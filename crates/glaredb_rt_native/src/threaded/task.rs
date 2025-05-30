use std::sync::Arc;
use std::sync::atomic::{self, AtomicBool, AtomicU8, AtomicUsize};
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
    pub(crate) sched_state: AtomicU8,
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
        // Set SCHEDULED bit.
        let old = self
            .sched_state
            .fetch_or(SCHEDULED, atomic::Ordering::AcqRel);

        if old & SCHEDULED == 0 {
            // Not previously scheduled, spawn...
            let state = self.clone();
            self.pool.spawn(move || {
                // As long as there's a pending wake, keep polling.
                loop {
                    // Clear pending.
                    state
                        .sched_state
                        .fetch_and(!PENDING, atomic::Ordering::AcqRel);

                    // Execute! (locks internally)
                    state.clone().execute();

                    // If nobody woke us in the meantime, we're done.
                    let bits = state.sched_state.load(atomic::Ordering::Acquire);
                    if bits & PENDING == 0 {
                        break;
                    }
                    // Otherwise there was at least one wake. Continue...
                    // execute again.
                }
                // Set no tasks inflight.
                let old = state
                    .sched_state
                    .fetch_and(!SCHEDULED, atomic::Ordering::AcqRel);
                if old & PENDING != 0 {
                    state.schedule();
                }
            });
        } else {
            // Already scheduled, so record that we got another wake.
            self.sched_state
                .fetch_or(PENDING, atomic::Ordering::Release);
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
