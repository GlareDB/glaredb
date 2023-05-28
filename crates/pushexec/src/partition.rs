use crate::errors::Result;
use crate::operator::{Sink, Source};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// A buffer of record batches for a partition, along with wakers awaiting
/// batches for said partition.
#[derive(Default, Debug)]
pub struct BufferedPartition {
    finished: bool,
    batches: Vec<RecordBatch>,
    wakers: Vec<Waker>,
}

impl BufferedPartition {
    /// Mark this partition as finished, waking all pending wakers.
    pub fn finish(&mut self) {
        assert!(!self.finished, "finished called twice");
        self.finished = true;
        self.drain_wake_all();
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn register_waker(&mut self, waker: Waker) {
        self.wakers.push(waker);
    }

    /// Push a batch for this partition, waking every register waker.
    pub fn push_batch(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
        self.drain_wake_all()
    }

    pub fn pop_batch(&mut self) -> Option<RecordBatch> {
        self.batches.pop()
    }

    fn drain_wake_all(&mut self) {
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

/// An adapter stream implementing DataFusion's record batch stream.
pub struct BufferedPartitionStream {
    schema: Arc<Schema>,
    inner: Arc<Mutex<BufferedPartition>>,
}

impl BufferedPartitionStream {
    pub fn new(
        schema: Arc<Schema>,
        inner: Arc<Mutex<BufferedPartition>>,
    ) -> BufferedPartitionStream {
        BufferedPartitionStream { schema, inner }
    }
}

impl Stream for BufferedPartitionStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.lock();
        match inner.pop_batch() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None if inner.finished => Poll::Ready(None),
            None => {
                inner.register_waker(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl RecordBatchStream for BufferedPartitionStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
