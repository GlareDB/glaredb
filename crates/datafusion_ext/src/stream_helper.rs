use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};

// Loosely enforces a limit for the number of records the stream will
// produce. Uses an atomic counter so that the limit can be enforced
// across partitions. Once the limit is surpassed no more record
// batches will be placed over the stream: this means that these
// streams will almost always produce slightly more records than the
// limit, essentially as a rounding error.
pub struct LimitingStreamAdapter {
    stream: SendableRecordBatchStream,
    counter: Arc<AtomicUsize>,
    limit: Option<usize>,
}

impl LimitingStreamAdapter {
    pub fn new(
        stream: SendableRecordBatchStream,
        counter: Arc<AtomicUsize>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            stream,
            counter,
            limit,
        }
    }
}


impl RecordBatchStream for LimitingStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

impl Stream for LimitingStreamAdapter {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(limit) = self.limit {
            if limit >= self.counter.load(Ordering::Relaxed) {
                return Poll::Ready(None);
            }
        }
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(res)) => Poll::Ready(Some(match res {
                Ok(rb) => {
                    self.counter.fetch_add(rb.num_rows(), Ordering::Relaxed);
                    Ok(rb)
                }
                Err(e) => Err(e),
            })),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
