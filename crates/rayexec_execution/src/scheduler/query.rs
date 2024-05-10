use crate::execution::pipeline::PartitionPipeline;
use parking_lot::Mutex;
use rayon::ThreadPool;
use std::{
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

/// Task for executing a partition pipeline.
pub struct PartitionPipelineTask {
    /// The partition pipeline we're operating on.
    pipeline: Arc<Mutex<PartitionPipeline>>,
}

impl PartitionPipelineTask {
    pub(crate) fn new(pipeline: PartitionPipeline) -> Self {
        PartitionPipelineTask {
            pipeline: Arc::new(Mutex::new(pipeline)),
        }
    }

    pub(crate) fn execute(self, pool: Arc<ThreadPool>) {
        // TODO: The arc+mutex around the pipeline could be removed here, but it
        // would increase complexity around the "pending" state.
        //
        // The idea would be construct a waker that _doesn't_ have a reference
        // to the pipeline, but then in the case of a "pending", we would
        // retroactively add it to the waker (the waker would have something
        // like `Arc<Mutex<Option<PartitionPipeline>>>`).
        //
        // However I didn't really feel like we need that yet. I'd rather have a
        // baseline on how this performs first (since the mutex will almost
        // always be uncontended).
        //
        // Also we would have to handle the race between the operator storing
        // the waker, and us actually putting the pipeline in the waker. This
        // could be covered by a bool like `did_wake` that we would check,
        // but... not right now.
        let mut pipeline = self.pipeline.lock();

        let waker: Waker = Arc::new(PartitionPipelineWaker {
            pipeline: self.pipeline.clone(),
            pool,
        })
        .into();

        let mut cx = Context::from_waker(&waker);
        loop {
            match pipeline.poll_execute(&mut cx) {
                Poll::Ready(Some(Ok(()))) => {
                    // Pushing through the pipeline was successful. Continue the
                    // loop to try to get as much work done as possible.
                    continue;
                }
                Poll::Ready(Some(Err(e))) => panic!("Schedule error: {e}"), // TODO: This should write the error somewhere.
                Poll::Pending => {
                    // Exit the loop. Waker was already stored in the pending
                    // sink/source, we'll be woken back up when there's more
                    // this operator chain can start executing.
                    return;
                }
                Poll::Ready(None) => {
                    // Exit the loop, nothing else for us to do. Waker is not
                    // stored, and we will not executed again.
                    return;
                }
            }
        }
    }
}

/// A waker implementation that will re-execute the pipeline once woken.
struct PartitionPipelineWaker {
    /// The pipeline we should re-execute.
    pipeline: Arc<Mutex<PartitionPipeline>>,

    /// The thread pool to spawn on.
    pool: Arc<ThreadPool>,
}

impl Wake for PartitionPipelineWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let task = PartitionPipelineTask {
            pipeline: self.pipeline.clone(),
        };
        let pool = self.pool.clone();
        self.pool.spawn(|| task.execute(pool));
    }
}
