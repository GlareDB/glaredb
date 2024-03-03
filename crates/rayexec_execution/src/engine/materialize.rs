use futures::Stream;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::{physical::plans::Sink2, types::batch::DataBatch};

/// Stream for materialized batches for a query.
#[derive(Debug)]
pub struct MaterializedBatchStream {
    state: Arc<Mutex<MaterializedBatchesState>>,
    sink: Option<MaterializedBatchSink>,
}

impl MaterializedBatchStream {
    /// Take the configured sink for the stream. Cannot be taken more than once.
    pub(crate) fn take_sink(&mut self) -> Result<Box<dyn Sink2>> {
        match self.sink.take() {
            Some(sink) => Ok(Box::new(sink)),
            None => Err(RayexecError::new("Attempted to take sink more than once")),
        }
    }
}

#[derive(Debug)]
struct MaterializedBatchesState {
    /// The materialized batches.
    batches: VecDeque<DataBatch>,
    /// Pending waker for the async stream implementation.
    waker: Option<Waker>,
    /// Whether or not the sink is finished.
    finished: bool,
}

impl MaterializedBatchStream {
    pub fn new() -> Self {
        let state = Arc::new(Mutex::new(MaterializedBatchesState {
            batches: VecDeque::new(),
            waker: None,
            finished: false,
        }));

        let sink = MaterializedBatchSink {
            state: state.clone(),
        };

        MaterializedBatchStream {
            state,
            sink: Some(sink),
        }
    }
}

#[derive(Debug)]
struct MaterializedBatchSink {
    state: Arc<Mutex<MaterializedBatchesState>>,
}

impl Sink2 for MaterializedBatchSink {
    fn push(&self, input: DataBatch, child: usize, partition: usize) -> Result<()> {
        if child != 0 {
            return Err(RayexecError::new(format!("non-zero child")));
        }
        if partition != 0 {
            return Err(RayexecError::new(format!("non-zero partition")));
        }

        let mut inner = self.state.lock();
        inner.batches.push_back(input);
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
        Ok(())
    }

    fn finish(&self, child: usize, partition: usize) -> Result<()> {
        if child != 0 {
            return Err(RayexecError::new(format!("non-zero child")));
        }
        if partition != 0 {
            return Err(RayexecError::new(format!("non-zero partition")));
        }

        let mut inner = self.state.lock();
        inner.finished = true;
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
        Ok(())
    }
}

impl Stream for MaterializedBatchStream {
    type Item = DataBatch;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut inner = self.state.lock();

        match inner.batches.pop_front() {
            Some(batch) => Poll::Ready(Some(batch)),
            None => {
                if inner.finished {
                    return Poll::Ready(None);
                }

                inner.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
