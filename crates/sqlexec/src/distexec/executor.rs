use std::sync::Arc;
use std::task::Poll;
use std::{
    fmt::Debug,
    task::{Context, Wake},
};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::scheduler::Scheduler;
use super::{
    pipeline::{PipelineStage, Sink, Source},
    DistExecError, Result,
};

#[derive(Debug, Clone)]
pub struct Task {
    /// Reference to a scheduler to enable rescheduling this task.
    pub scheduler: Scheduler,

    /// The stage to execute.
    pub stage: PipelineStage,

    /// The partition of the stage to execute.
    pub partition: usize,
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

    fn execute_inner(task: Task) {
        // TODO: Check canceled.

        let partition = task.partition;

        let waker = Arc::new(TaskWaker { task });
        let c_waker = waker.clone().into();
        let mut cx = Context::from_waker(&c_waker);

        match waker.task.stage.source.poll_partition(&mut cx, partition) {
            Poll::Ready(Some(Ok(batch))) => {
                // waker.task.stage.output.push(batch, 0, partition).unwrap();

                // Reschedule this task to keep execution going.
                waker.task.clone().reschedule();
            }
            Poll::Ready(Some(Err(e))) => {
                //
                panic!("{}", e);
            }
            Poll::Ready(None) => {
                // waker.task.stage.output.finish(0, partition).unwrap();
                //
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
