use crate::{
    errors::{PushExecError, Result},
    pipeline::PushPartitionId,
    plan::MetaPipeline,
};
use rayon::ThreadPool;
use std::task::{Poll, Wake};
use std::{sync::Arc, task::Context};

pub struct Scheduler {
    sync_pool: Arc<ThreadPool>,
}

impl Scheduler {
    pub fn new(sync_pool: Arc<ThreadPool>) -> Scheduler {
        Scheduler { sync_pool }
    }

    pub fn spawn_pipeline(&self, meta: MetaPipeline) -> Result<()> {
        let pool = self.sync_pool.clone();
        self.sync_pool.install(|| spawn_pipeline(meta, pool))
    }
}

/// Spawn and execute a meta pipeline for a query.
fn spawn_pipeline(meta: MetaPipeline, sync_pool: Arc<ThreadPool>) -> Result<()> {
    let context = Arc::new(TaskContext {
        meta,
        sync_pool: sync_pool.clone(),
    });

    for (pipeline_idx, pipeline) in context.meta.pipelines.iter().enumerate() {
        for partition in 0..pipeline.pipeline.output_partitions() {
            let task = PartitionTask {
                pipeline: pipeline_idx,
                partition,
                context: context.clone(),
                waker: Arc::new(PartitionWaker {
                    pipeline: pipeline_idx,
                    partition,
                    context: context.clone(), // TODO: ?
                }),
            };
            sync_pool.spawn(|| task.execute());
        }
    }

    Ok(())
}

struct TaskContext {
    /// The meta pipeline to reference during execution.
    meta: MetaPipeline,
    /// Rayon thread pool to schedule tasks on.
    // TODO: Store tokio thread pool too for io bound tasks.
    sync_pool: Arc<ThreadPool>,
}

struct PartitionTask {
    /// Index of the pipeline this worker is executing for.
    pipeline: usize,
    /// Partition of the pipeline this worker is executing for.
    partition: usize,
    context: Arc<TaskContext>,
    waker: Arc<PartitionWaker>,
}

impl PartitionTask {
    /// Execute a task.
    fn execute(self) {
        let pipeline = &self.context.meta.pipelines[self.pipeline];

        let waker = self.waker.into();
        let mut cx = Context::from_waker(&waker);

        match pipeline.pipeline.poll_partition(&mut cx, self.partition) {
            Poll::Ready(Some(Ok(batch))) => {
                match pipeline.dest {
                    Some(dest) => self.context.meta.pipelines[dest.pipeline]
                        .pipeline
                        .push_partition(
                            batch,
                            PushPartitionId {
                                idx: dest.pipeline,
                                child: dest.child, // TODO
                            },
                        )
                        .unwrap(), // TODO
                    None => {
                        if let Some(sink) = &self.context.meta.sink {
                            sink.push_partition(
                                batch,
                                PushPartitionId {
                                    idx: self.partition,
                                    child: 0,
                                },
                            )
                            .unwrap();
                            // TODO
                        }
                    }
                }
            }
            Poll::Ready(Some(Err(e))) => {
                //
                panic!("err: {}", e)
            }
            Poll::Ready(None) => {
                match pipeline.dest {
                    Some(dest) => {
                        self.context.meta.pipelines[dest.pipeline]
                            .pipeline
                            .finish(PushPartitionId {
                                idx: dest.pipeline,
                                child: dest.child,
                            })
                            .unwrap() // TODO
                    }
                    None => {
                        if let Some(sink) = &self.context.meta.sink {
                            sink.finish(PushPartitionId {
                                idx: self.partition,
                                child: 0,
                            })
                            .unwrap() // TODO
                        }
                    }
                }
            }
            Poll::Pending => {
                //
            }
        }
    }
}

/// A waker responsible for spawning a task for working on a partition.
///
/// There should only exist a single waker per partition, and there should only
/// ever be one task running per partition at a time.
struct PartitionWaker {
    /// Index of the pipeline this worker is executing for.
    pipeline: usize,
    /// Partition of the pipeline this worker is executing for.
    partition: usize,
    context: Arc<TaskContext>,
}

impl Wake for PartitionWaker {
    fn wake(self: Arc<Self>) {
        let task = PartitionTask {
            pipeline: self.pipeline,
            partition: self.partition,
            context: self.context.clone(),
            waker: self.clone(),
        };
        self.context.sync_pool.spawn(|| task.execute())
    }
}
