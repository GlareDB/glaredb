use datafusion::arrow::{datatypes::Schema, record_batch::RecordBatch};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::{Partitioning, RecordBatchStream};
use futures::channel::mpsc;
use futures::{ready, Stream, StreamExt};
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::pipeline::{ErrorSink, Sink};
use super::{DistExecError, Result};

/// Constructs an adapter sink that can be used to capture the output of a
/// pipeline, and delivers it to an adapter stream that implements
/// `RecordBatchStream`.
pub fn create_coalescing_adapter(
    partition: Partitioning,
    schema: Arc<Schema>,
) -> (CoalescingAdapterSink, AdapterStream) {
    let (tx, rx) = mpsc::unbounded();
    let closed = Mutex::new(vec![false; partition.partition_count()]);

    let sink = CoalescingAdapterSink { tx, closed };
    let stream = AdapterStream { rx, schema };

    (sink, stream)
}

/// A sink implementation that coalesces all batch partitions into a single
/// stream.
#[derive(Debug)]
pub struct CoalescingAdapterSink {
    tx: mpsc::UnboundedSender<Option<Result<RecordBatch>>>,
    /// Tracks closed input partitions.
    closed: Mutex<Vec<bool>>,
}

impl Sink for CoalescingAdapterSink {
    fn push(&self, input: RecordBatch, child: usize, _partition: usize) -> Result<()> {
        if child != 0 {
            return Err(DistExecError::String(
                "Adapater sink can only accept input from one child".to_string(),
            ));
        }

        if self.tx.unbounded_send(Some(Ok(input))).is_err() {
            return Err(DistExecError::String(
                "Failed to send batch on channel".to_string(),
            ));
        }

        Ok(())
    }

    fn finish(&self, child: usize, partition: usize) -> Result<()> {
        if child != 0 {
            return Err(DistExecError::String(
                "Adapater sink can only accept input from one child".to_string(),
            ));
        }

        let mut closed = self.closed.lock();
        closed[partition] = true;

        // Indicate that this stream is done if all input partitions are closed.
        if closed.iter().all(|b| *b) && self.tx.unbounded_send(None).is_err() {
            return Err(DistExecError::String(
                "Failed to close msg on channel".to_string(),
            ));
        }

        Ok(())
    }
}

impl ErrorSink for CoalescingAdapterSink {
    fn push_error(&self, err: DistExecError, _partition: usize) -> Result<()> {
        if let Err(e) = self.tx.unbounded_send(Some(Err(err))) {
            return Err(DistExecError::String(format!(
                "Failed to send error on channel: {e}"
            )));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct AdapterStream {
    rx: mpsc::UnboundedReceiver<Option<Result<RecordBatch>>>,
    schema: Arc<Schema>,
}

impl Stream for AdapterStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let opt = ready!(self.rx.poll_next_unpin(cx)).flatten();
        Poll::Ready(opt.map(|r| r.map_err(|e| DataFusionError::External(Box::new(e)))))
    }
}

impl RecordBatchStream for AdapterStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
