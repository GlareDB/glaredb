use std::fmt::Debug;
use std::sync::Arc;
use std::task::{Context, Poll, Wake};

use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error};

use super::pipeline::{ErrorSink, Sink, Source};
use super::scheduler::Scheduler;
use super::DistExecError;

#[derive(Debug, Clone)]
pub struct Task {
    /// Reference to a scheduler to enable rescheduling this task.
    pub scheduler: Scheduler,

    /// The source to execute.
    pub source: Arc<dyn Source>,

    /// Output sink.
    pub output: Arc<dyn Sink>,

    /// Error sink.
    pub errors: Arc<dyn ErrorSink>,

    /// The partition of the stage to execute.
    pub partition: usize,

    /// This index of this node in relation to the parent query node.
    pub child: usize,
}

impl Task {
    pub fn reschedule(self) {
        let scheduler = self.scheduler.clone();
        scheduler.schedule_task(self)
    }
}

/// A local executor backed by a single tokio thread.
#[derive(Debug)]
pub struct TaskExecutor {
    _handle: JoinHandle<()>,
}

impl TaskExecutor {
    pub fn new(
        local_task_receiver: async_channel::Receiver<Task>,
        mut remote_task_receiver: Option<mpsc::Receiver<Task>>,
    ) -> Self {
        /// Wait for task from both local and remote queue and execute them.
        ///
        /// Prefers tasks from local queue ahead of remote queue.
        async fn get_task(
            local_task_receiver: &async_channel::Receiver<Task>,
            remote_task_receiver: &mut mpsc::Receiver<Task>,
        ) -> Option<Task> {
            match local_task_receiver.try_recv() {
                Ok(task) => return Some(task),
                Err(async_channel::TryRecvError::Empty) => (),
                Err(async_channel::TryRecvError::Closed) => {
                    return None;
                }
            };

            match remote_task_receiver.try_recv() {
                Ok(task) => return Some(task),
                Err(mpsc::error::TryRecvError::Empty) => (),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return None;
                }
            }

            select! {
                local_task = local_task_receiver.recv() => local_task.ok(),
                remote_task = remote_task_receiver.recv() => remote_task,
            }
        }

        let handle = tokio::spawn(async move {
            loop {
                let task = if let Some(remote_task_receiver) = remote_task_receiver.as_mut() {
                    get_task(&local_task_receiver, remote_task_receiver).await
                } else {
                    local_task_receiver.recv().await.ok()
                };

                if let Some(task) = task {
                    Self::execute_inner(task);
                } else {
                    // Exit if we don't receive a task. It's just a shutdown.
                    debug!("Exiting task executor");
                    break;
                }
            }
        });

        Self { _handle: handle }
    }

    /// Handle an error for a particular child/partition. This will attempt to
    /// push the error to `error_sink` then finish the partition for `sink`.
    fn handle_error(
        child: usize,
        partition: usize,
        sink: &dyn Sink,
        error_sink: &dyn ErrorSink,
        err: DistExecError,
    ) {
        debug!(%err, %partition, "handling error for failed execution");
        if let Err(e) = error_sink.push_error(err, partition) {
            error!(%e, %partition, "failed to push error for partition");
        }

        if let Err(e) = sink.finish(child, partition) {
            error!(%e, %partition, "failed to mark partition as finished after pushing error");
        }
    }

    fn execute_inner(task: Task) {
        // TODO: Check canceled.

        let partition = task.partition;
        let child = task.child;

        let waker = Arc::new(TaskWaker { task });
        let c_waker = waker.clone().into();
        let mut cx = Context::from_waker(&c_waker);

        // Try to get the next batch for a partition.
        //
        // - If batch available, push to sink and reschedule task to keep executing.
        // - If stream finished, mark output as finished for this partition.
        // - If error, push error and mark output finished for this partition.
        // - If pending, reschedule on wake.
        match waker.task.source.poll_partition(&mut cx, partition) {
            Poll::Ready(Some(Ok(batch))) => {
                if let Err(e) = waker.task.output.push(batch, child, partition) {
                    Self::handle_error(
                        child,
                        partition,
                        waker.task.output.as_ref(),
                        waker.task.errors.as_ref(),
                        e,
                    );
                    // No rescheduling. This partition is dead to us.
                    return;
                }

                // Reschedule this task to keep execution going.
                waker.task.clone().reschedule();
            }
            Poll::Ready(Some(Err(e))) => {
                Self::handle_error(
                    child,
                    partition,
                    waker.task.output.as_ref(),
                    waker.task.errors.as_ref(),
                    e,
                );
                // No rescheduling. This partition is dead to us.
            }
            Poll::Ready(None) => {
                if let Err(e) = waker.task.output.finish(child, partition) {
                    Self::handle_error(
                        child,
                        partition,
                        waker.task.output.as_ref(),
                        waker.task.errors.as_ref(),
                        e,
                    );
                }
                // No rescheduling. This partition is dead to us.
            }
            Poll::Pending => {
                // Do nothing, our waker will take care of rescheduling once a
                // new partition batch is ready.

                // TODO: Actually do this in waker
                waker.task.clone().reschedule();
            }
        }
    }
}

/// A waker that will reschedule this task for execution on wake.
#[derive(Debug)]
struct TaskWaker {
    task: Task,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {}
}
