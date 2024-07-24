use std::fmt;
use std::sync::Arc;

use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_execution::functions::copy::{CopyToFunction, CopyToSink};
use rayexec_execution::runtime::ExecutionRuntime;
use rayexec_io::location::AccessConfig;
use rayexec_io::location::FileLocation;

use crate::writer::AsyncBatchWriter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetCopyToFunction;

impl CopyToFunction for ParquetCopyToFunction {
    fn name(&self) -> &'static str {
        "parquet_copy_to"
    }

    fn create_sinks(
        &self,
        runtime: &Arc<dyn ExecutionRuntime>,
        schema: Schema,
        location: FileLocation,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn CopyToSink>>> {
        let provider = runtime.file_provider();

        let mut sinks = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            let sink = provider.file_sink(location.clone(), &AccessConfig::None)?;
            let writer = AsyncBatchWriter::try_new(sink, schema.clone())?;
            sinks.push(Box::new(ParquetCopyToSink { writer }) as _)
        }

        Ok(sinks)
    }
}

pub struct ParquetCopyToSink {
    writer: AsyncBatchWriter,
}

impl ParquetCopyToSink {
    async fn push_inner(&mut self, batch: Batch) -> Result<()> {
        self.writer.write(&batch).await?;
        Ok(())
    }

    async fn finalize_inner(&mut self) -> Result<()> {
        self.writer.finish().await?;
        Ok(())
    }
}

impl CopyToSink for ParquetCopyToSink {
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
        self.push_inner(batch).boxed()
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        self.finalize_inner().boxed()
    }
}

impl fmt::Debug for ParquetCopyToSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetCopyToSink").finish_non_exhaustive()
    }
}
