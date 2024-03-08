use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};

/// Thin wrapper around a record batch stream that automatically records metrics
/// about batches that are sent through the stream.
///
/// Note this should only be used when "ingesting" data during execution (data
/// sources or reading from tables) to avoid double counting bytes read.
pub struct LimitingStreamAdapter {
    stream: SendableRecordBatchStream,
    counter: Arc<AtomicUsize>,
    limit: Option<usize>,
}

impl LimitingStreamAdapter {
    /// Create a new stream with a new set of data source metrics for the given
    /// partition.
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

impl RecordBatchStream for LimitingStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}
