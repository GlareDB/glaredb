use crate::errors::PushExecError;
use crate::errors::Result;
use crate::pipeline::{Pipeline, Sink, Source};
use crate::plan::{PipelinePlan, RoutablePipeline};
use crate::Spawner;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::{ArrowError, Result as ArrowResult};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::channel::mpsc;
use futures::task::ArcWake;
use futures::{ready, Stream, StreamExt};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::Wake;
use std::task::{Context, Poll};

/// Spawns a `PipelinePlan` using the provided `Spawner`
pub fn spawn_plan(plan: PipelinePlan, spawner: Spawner) -> ExecutionResults {
    let (senders, receivers) = (0..plan.output_partitions)
        .map(|_| mpsc::unbounded())
        .unzip::<_, _, Vec<_>, Vec<_>>();

    let context = Arc::new(ExecutionContext {
        spawner,
        pipelines: plan.pipelines,
        schema: plan.schema,
        output: senders,
    });

    for (pipeline_idx, query_pipeline) in context.pipelines.iter().enumerate() {
        for partition in 0..query_pipeline.pipeline.output_partitions() {
            context.spawner.spawn(Task {
                context: context.clone(),
                waker: Arc::new(TaskWaker {
                    context: Arc::downgrade(&context),
                    pipeline: pipeline_idx,
                    partition,
                }),
            });
        }
    }

    let partitions = receivers
        .into_iter()
        .map(|receiver| ExecutionResultStream {
            receiver,
            context: context.clone(),
        })
        .collect();

    ExecutionResults {
        streams: partitions,
        context,
    }
}

/// A [`Task`] identifies an output partition within a given pipeline that may be able to
/// make progress. The [`Scheduler`][super::Scheduler] maintains a list of outstanding
/// [`Task`] and distributes them amongst its worker threads.
pub struct Task {
    /// Maintain a link to the [`ExecutionContext`] this is necessary to be able to
    /// route the output of the partition to its destination
    context: Arc<ExecutionContext>,

    /// A [`ArcWake`] that can be used to re-schedule this [`Task`] for execution
    waker: Arc<TaskWaker>,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let output = &self.context.pipelines[self.waker.pipeline].output;

        f.debug_struct("Task")
            .field("pipeline", &self.waker.pipeline)
            .field("partition", &self.waker.partition)
            .field("output", &output)
            .finish()
    }
}

impl Task {
    fn handle_error(&self, partition: usize, routable: &RoutablePipeline, error: PushExecError) {
        self.context.send_query_output(partition, Err(error));
        if let Some(link) = routable.output {
            self.context.pipelines[link.pipeline]
                .pipeline
                .close(link.child, self.waker.partition);
        }
    }

    /// Call [`Pipeline::poll_partition`], attempting to make progress on query execution
    pub fn do_work(self) {
        if self.context.is_cancelled() {
            return;
        }

        let node = self.waker.pipeline;
        let partition = self.waker.partition;

        let waker = self.waker.clone().into();
        let mut cx = Context::from_waker(&waker);

        let pipelines = &self.context.pipelines;
        let routable = &pipelines[node];
        match routable.pipeline.poll_partition(&mut cx, partition) {
            Poll::Ready(Some(Ok(batch))) => {
                match routable.output {
                    Some(link) => {
                        let r = pipelines[link.pipeline]
                            .pipeline
                            .push(batch, link.child, partition);

                        if let Err(e) = r {
                            // Return without rescheduling this output again
                            return;
                        }
                    }
                    None => self.context.send_query_output(partition, Ok(batch)),
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
                spawner.spawn(self);
            }
            Poll::Ready(Some(Err(e))) => {
                //
            }
            Poll::Ready(None) => match routable.output {
                Some(link) => pipelines[link.pipeline]
                    .pipeline
                    .close(link.child, partition),
                None => self.context.finish(partition),
            },
            Poll::Pending => {
                //
            }
        }
    }
}

/// The results of the execution of a query
pub struct ExecutionResults {
    /// [`ExecutionResultStream`] for each partition of this query
    streams: Vec<ExecutionResultStream>,

    /// Keep a reference to the [`ExecutionContext`] so it isn't dropped early
    context: Arc<ExecutionContext>,
}

impl ExecutionResults {
    /// Returns a [`SendableRecordBatchStream`] of this execution
    ///
    /// In the event of multiple output partitions, the output will be interleaved
    pub fn stream(self) -> SendableRecordBatchStream {
        let schema = self.context.schema.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::select_all(self.streams),
        ))
    }

    /// Returns a [`SendableRecordBatchStream`] for each partition of this execution
    pub fn stream_partitioned(self) -> Vec<SendableRecordBatchStream> {
        self.streams.into_iter().map(|s| Box::pin(s) as _).collect()
    }
}

/// A result stream for the execution of a query
struct ExecutionResultStream {
    receiver: mpsc::UnboundedReceiver<Option<Result<RecordBatch>>>,

    /// Keep a reference to the [`ExecutionContext`] so it isn't dropped early
    context: Arc<ExecutionContext>,
}

impl Stream for ExecutionResultStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let opt = ready!(self.receiver.poll_next_unpin(cx)).flatten();
        Poll::Ready(opt.map(|r| r.map_err(|e| DataFusionError::External(Box::new(e)))))
    }
}

impl RecordBatchStream for ExecutionResultStream {
    fn schema(&self) -> SchemaRef {
        self.context.schema.clone()
    }
}

/// The shared state of all [`Task`] created from the same [`PipelinePlan`]
#[derive(Debug)]
struct ExecutionContext {
    /// Spawner for this query
    spawner: Spawner,

    /// List of pipelines that belong to this query, pipelines are addressed
    /// based on their index within this list
    pipelines: Vec<RoutablePipeline>,

    /// Schema of this plans output
    pub schema: SchemaRef,

    /// The output streams, per partition, for this query's execution
    output: Vec<mpsc::UnboundedSender<Option<Result<RecordBatch>>>>,
}

impl ExecutionContext {
    /// Returns `true` if this query has been dropped, specifically if the
    /// stream returned by [`super::Scheduler::schedule`] has been dropped
    fn is_cancelled(&self) -> bool {
        self.output.iter().all(|x| x.is_closed())
    }

    /// Sends `output` to this query's output stream
    fn send_query_output(&self, partition: usize, output: Result<RecordBatch>) {
        let _ = self.output[partition].unbounded_send(Some(output));
    }

    /// Mark this partition as finished
    fn finish(&self, partition: usize) {
        let _ = self.output[partition].unbounded_send(None);
    }
}

struct TaskWaker {
    /// Store a weak reference to the [`ExecutionContext`] to avoid reference cycles if this
    /// [`Waker`] is stored within a [`Pipeline`] owned by the [`ExecutionContext`]
    context: Weak<ExecutionContext>,

    /// The index of the pipeline within `query` to poll
    pipeline: usize,

    /// The partition of the pipeline within `query` to poll
    partition: usize,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        if let Some(context) = self.context.upgrade() {
            let task = Task {
                context,
                waker: self.clone(),
            };

            task.context.spawner.clone().spawn(task);
        }
    }
}
