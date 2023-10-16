use std::sync::Arc;
use std::task::Poll;
use std::{
    fmt::Debug,
    task::{Context, Wake},
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error};

use super::pipeline::ErrorSink;
use super::scheduler::Scheduler;
use super::{
    pipeline::{PipelineStage, Sink, Source},
    DistExecError, Result,
};

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

pub trait TaskExecutor: Sync + Send + Debug {
    /// Begin execution of a task.
    ///
    /// This should return immediately as to not block the scheduler loop. The
    /// task should be handed off to some worker. Upon completion, the executor
    /// should signal to the scheduler that it's ready for more work.
    fn execute(self, task: Task);
}

/// A local executor backed by a single tokio thread.
#[derive(Debug)]
pub struct LocalTaskExecutor {
    send: mpsc::Sender<(Self, Task)>,
    _handle: JoinHandle<()>,
}

impl LocalTaskExecutor {
    pub fn new() -> Self {
        let (send, mut recv) = mpsc::channel::<(Self, Task)>(1);
        let handle = tokio::spawn(async move {
            // Main work loop, execute task, then push the executor back to the
            // scheduler to receive more work.
            while let Some((exec, task)) = recv.recv().await {
                let scheduler = task.scheduler.clone();
                Self::execute_inner(task);
                scheduler.register_local_executor(exec);
            }
        });
        Self {
            send,
            _handle: handle,
        }
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
            }
        }
    }
}

impl TaskExecutor for LocalTaskExecutor {
    fn execute(self, task: Task) {
        let send = self.send.clone();
        send.try_send((self, task)).unwrap();
    }
}

/// A waker that will reschedule this task for execution on wake.
#[derive(Debug)]
struct TaskWaker {
    task: Task,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.task.clone().reschedule();
    }
}
