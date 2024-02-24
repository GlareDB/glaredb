use futures::Stream;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use crate::{physical::plans::Sink, types::batch::DataBatch};

/// Implements a physical Sink for getting the materialized batches for a query.
#[derive(Debug)]
pub struct MaterializedBatches {
    inner: Mutex<MaterializedBatchesInner>,
}

#[derive(Debug)]
struct MaterializedBatchesInner {
    /// The materialized batches.
    batches: VecDeque<DataBatch>,
    /// Pending waker for the async stream implementation.
    waker: Option<Waker>,
    /// Whether or not the sink is finished.
    finished: bool,
}

impl MaterializedBatches {
    pub fn new() -> Self {
        MaterializedBatches {
            inner: Mutex::new(MaterializedBatchesInner {
                batches: VecDeque::new(),
                waker: None,
                finished: false,
            }),
        }
    }
}

impl Sink for MaterializedBatches {
    fn push(&self, input: DataBatch, child: usize, partition: usize) -> Result<()> {
        if child != 0 {
            return Err(RayexecError::new(format!("non-zero child")));
        }
        if partition != 0 {
            return Err(RayexecError::new(format!("non-zero partition")));
        }

        let mut inner = self.inner.lock();
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

        let mut inner = self.inner.lock();
        inner.finished = true;
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
        Ok(())
    }
}

impl Stream for MaterializedBatches {
    type Item = DataBatch;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.lock();

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
