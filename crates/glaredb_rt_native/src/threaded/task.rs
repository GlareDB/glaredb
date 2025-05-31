use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use glaredb_core::execution::partition_pipeline::ExecutablePartitionPipeline;
use glaredb_core::runtime::pipeline::ErrorSink;
use glaredb_core::runtime::profile_buffer::ProfileSink;
use glaredb_error::DbError;
use parking_lot::Mutex;
use rayon::ThreadPool;

use crate::time::NativeInstant;

#[derive(Debug)]
pub(crate) struct ScheduleState {
    pub(crate) running: bool,
    pub(crate) pending: bool,
    pub(crate) completed: bool,
    pub(crate) canceled: bool,
}

#[derive(Debug)]
pub(crate) struct TaskState {
    /// The partition pipeline we're operating on.
    pub(crate) pipeline: Mutex<ExecutablePartitionPipeline>,
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
        if sched_guard.completed {
            return;
        }
        if sched_guard.canceled {
            self.errors.set_error(DbError::new("Query canceled"));
            return;
        }

        if sched_guard.running {
            sched_guard.pending = true;
        } else {
            sched_guard.running = true;
            std::mem::drop(sched_guard);

            let state = self.clone();
            self.pool.spawn(move || {
                loop {
                    let completed = state.execute();

                    let mut sched_guard = state.sched_state.lock();
                    sched_guard.completed = completed;
                    if sched_guard.pending {
                        sched_guard.pending = false;
                        // Only continue if we're not complete.
                        if completed {
                            break;
                        }
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

    fn execute(self: &Arc<Self>) -> bool {
        let mut pipeline = self.pipeline.lock();

        let waker: Waker = self.clone().into();
        let mut cx = Context::from_waker(&waker);

        match pipeline.poll_execute::<NativeInstant>(&mut cx) {
            Poll::Ready(Ok(prof)) => {
                // Pushing through the pipeline was successful. Put our profile.
                // We'll never execute again.
                self.profile_sink.put(prof);
                true
            }
            Poll::Ready(Err(e)) => {
                self.errors.set_error(e);
                false
            }
            Poll::Pending => {
                //  Waker was already stored in the pending
                // sink/source, we'll be woken back up when there's more
                // this operator chain can start executing.
                false
            }
        }
    }
}
