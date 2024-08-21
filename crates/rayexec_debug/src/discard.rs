use futures::future::BoxFuture;
use rayexec_bullet::{batch::Batch, field::Schema};
use rayexec_error::Result;
use rayexec_execution::{
    execution::operators::sink::PartitionSink, functions::copy::CopyToFunction,
};
use rayexec_io::location::FileLocation;

/// COPY TO function implementation that discards all input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiscardCopyToFunction;

impl CopyToFunction for DiscardCopyToFunction {
    fn name(&self) -> &'static str {
        "discard_copy_to"
    }

    fn create_sinks(
        &self,
        _schema: Schema,
        _location: FileLocation,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        let sinks = (0..num_partitions)
            .map(|_| Box::new(DiscardCopyToSink) as _)
            .collect();

        Ok(sinks)
    }
}

#[derive(Debug, Clone, Copy)]
struct DiscardCopyToSink;

impl PartitionSink for DiscardCopyToSink {
    fn push(&mut self, _batch: Batch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
