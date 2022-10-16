//! Modify partition streams.
use crate::errors::Result;
use crate::keys::PartitionKey;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::future::BoxFuture;

pub trait StreamModifierOpener<M: StreamModifier>: Unpin {
    /// Opens a `StreamModifier` for some partition.
    ///
    /// NOTE: The boxed future will be useful if/when we need to read deltas
    /// from disk.
    fn open_modifier(
        &self,
        partition: &PartitionKey,
        output_schema: &SchemaRef,
    ) -> Result<BoxFuture<'static, M>>;
}

/// Describes how we modify a stream. Works on a record batch at a time.
pub trait StreamModifier: Unpin {
    /// Given a record batch, make any necessary modifications to it.
    fn modify(&self, batch: RecordBatch) -> Result<RecordBatch>;

    /// Return a stream for any remaining batches we need to send.
    ///
    /// Useful for returning data that has not yet been flushed to the
    /// underlying partition store.
    fn stream_rest(&self) -> SendableRecordBatchStream;
}
