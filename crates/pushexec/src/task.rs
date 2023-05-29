use crate::errors::PushExecError;
use crate::plan::{PipelinePlan, RoutablePipeline};
use crate::Spawner;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Wake;
use std::task::{Context, Poll};
use tracing::{debug, error};

/// Spawns a `PipelinePlan` using the provided `Spawner`
pub fn spawn_plan(plan: PipelinePlan, spawner: Spawner) {
    let context = Arc::new(ExecutionContext { spawner, plan });

    for (pipeline_idx, query_pipeline) in context.plan.pipelines.iter().enumerate() {
        for partition in 0..query_pipeline.pipeline.output_partitions() {
            context.spawner.spawn(Task {
                context: context.clone(),
                waker: Arc::new(TaskWaker {
                    context: context.clone(),
                    wake_count: AtomicUsize::new(1),
                    pipeline: pipeline_idx,
                    partition,
                }),
            });
        }
    }
}

/// A `Task` identifies an output partition within a given pipeline that may be
/// able to make progress. The `Scheduler` maintains a list of outstanding
/// `Task` and distributes them amongst its worker threads.
pub struct Task {
    /// Maintain a link to the `ExecutionContext` this is necessary to be able
    /// to route the output of the partition to its destination.
    context: Arc<ExecutionContext>,

    /// A waker that can be used to re-schedule this `Task` for execution.
    waker: Arc<TaskWaker>,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let output = &self.context.plan.pipelines[self.waker.pipeline].output;

        f.debug_struct("Task")
            .field("pipeline", &self.waker.pipeline)
            .field("partition", &self.waker.partition)
            .field("output", &output)
            .finish()
    }
}

impl Task {
    fn handle_error(&self, partition: usize, routable: &RoutablePipeline, error: PushExecError) {
        debug!(%error, ?routable, "handling error for pipeline");
        if let Err(e) = self
            .context
            .plan
            .error_sink
            .push_error(error, self.waker.partition)
        {
            error!(%partition, %e, "failed to push error for partition")
        }

        if let Some(link) = routable.output {
            if let Err(e) = self.context.plan.pipelines[link.pipeline]
                .pipeline
                .close(link.child, self.waker.partition)
            {
                error!(?link, %e, "failed to close pipeline");
            }
        }
    }

    /// Call [`Pipeline::poll_partition`], attempting to make progress on query execution
    pub fn do_work(self) {
        if self.context.plan.is_cancelled() {
            debug!("pipeline cancelled");
            return;
        }

        // Capture the wake count prior to calling [`Pipeline::poll_partition`]
        // this allows us to detect concurrent wake ups and handle them correctly
        let wake_count = self.waker.wake_count.load(Ordering::SeqCst);

        let node = self.waker.pipeline;
        let partition = self.waker.partition;

        let waker = self.waker.clone().into();
        let mut cx = Context::from_waker(&waker);

        let pipelines = &self.context.plan.pipelines;
        let routable = &pipelines[node];
        match routable.pipeline.poll_partition(&mut cx, partition) {
            Poll::Ready(Some(Ok(batch))) => {
                match routable.output {
                    Some(link) => {
                        if let Err(e) = pipelines[link.pipeline]
                            .pipeline
                            .push(batch, link.child, partition)
                        {
                            self.handle_error(partition, routable, e);
                            // Return without rescheduling this output again.
                            return;
                        }
                    }
                    None => {
                        if let Err(e) = self.context.plan.sink.push(batch, 0, partition) {
                            self.handle_error(partition, routable, e);
                        }
                    }
                }

                // Reschedule this pipeline again
                //
                // We want to prioritise running tasks triggered by the most
                // recent batch, so reschedule with FIFO ordering
                //
                // Note: We must schedule after we have routed the batch,
                // otherwise we introduce a potential ordering race where the
                // newly scheduled task runs before this task finishes routing
                // the output
                let spawner = self.context.spawner.clone();
                spawner.spawn_fifo(self);
            }
            Poll::Ready(Some(Err(e))) => self.handle_error(partition, routable, e),
            Poll::Ready(None) => match routable.output {
                Some(link) => {
                    if let Err(e) = pipelines[link.pipeline]
                        .pipeline
                        .close(link.child, partition)
                    {
                        self.handle_error(partition, routable, e)
                    }
                }
                None => {
                    if let Err(e) = self.context.plan.sink.close(0, partition) {
                        self.handle_error(partition, routable, e)
                    }
                }
            },
            Poll::Pending => {
                // Attempt to reset the wake count with the value obtained prior
                // to calling [`Pipeline::poll_partition`].
                //
                // If this fails it indicates a wakeup was received whilst executing
                // [`Pipeline::poll_partition`] and we should reschedule the task
                let reset = self.waker.wake_count.compare_exchange(
                    wake_count,
                    0,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );

                if reset.is_err() {
                    let spawner = self.context.spawner.clone();
                    spawner.spawn(self);
                }
            }
        }
    }
}

/// The shared state of all [`Task`] created from the same [`PipelinePlan`]
#[derive(Debug)]
struct ExecutionContext {
    /// Spawner for this query
    spawner: Spawner,

    /// The pipeline plan that's being executed for this query.
    plan: PipelinePlan,
}

struct TaskWaker {
    /// Execution context for this query.
    context: Arc<ExecutionContext>,

    /// A counter that stores the number of times this has been awoken
    ///
    /// A value > 0, implies the task is either in the ready queue or
    /// currently being executed
    ///
    /// `TaskWaker::wake` always increments the `wake_count`, however, it only
    /// re-enqueues the [`Task`] if the value prior to increment was 0
    ///
    /// This ensures that a given [`Task`] is not enqueued multiple times
    ///
    /// We store an integer, as opposed to a boolean, so that wake ups that
    /// occur during [`Pipeline::poll_partition`] can be detected and handled
    /// after it has finished executing
    wake_count: AtomicUsize,

    /// The index of the pipeline to poll.
    pipeline: usize,

    /// The partition of the pipeline to poll.
    partition: usize,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        if self.wake_count.fetch_add(1, Ordering::SeqCst) != 0 {
            return;
        }

        let task = Task {
            context: self.context.clone(),
            waker: self.clone(),
        };

        task.context.spawner.clone().spawn(task);
    }
}
