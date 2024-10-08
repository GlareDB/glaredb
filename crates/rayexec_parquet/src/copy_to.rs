use std::fmt;

use futures::future::BoxFuture;
use futures::FutureExt;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_execution::execution::operators::sink::PartitionSink;
use rayexec_execution::functions::copy::CopyToFunction;
use rayexec_execution::runtime::Runtime;
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::FileProvider;

use crate::writer::AsyncBatchWriter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetCopyToFunction<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> CopyToFunction for ParquetCopyToFunction<R> {
    fn name(&self) -> &'static str {
        "parquet_copy_to"
    }

    fn create_sinks(
        &self,
        schema: Schema,
        location: FileLocation,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        let provider = self.runtime.file_provider();

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

impl PartitionSink for ParquetCopyToSink {
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
