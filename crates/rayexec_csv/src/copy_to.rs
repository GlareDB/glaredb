use futures::future::BoxFuture;
use futures::FutureExt;
use rayexec_error::Result;
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::arrays::field::Schema;
use rayexec_execution::execution::operators::sink::PartitionSink;
use rayexec_execution::functions::copy::CopyToFunction;
use rayexec_execution::runtime::Runtime;
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::{FileProvider, FileSink};

use crate::reader::DialectOptions;
use crate::writer::CsvEncoder;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvCopyToFunction<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> CopyToFunction for CsvCopyToFunction<R> {
    fn name(&self) -> &'static str {
        "csv_copy_to"
    }

    // TODO: Access config
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
            let dialect = DialectOptions::default();

            sinks.push(Box::new(CsvCopyToSink {
                encoder: CsvEncoder::new(schema.clone(), dialect),
                sink,
            }) as _)
        }

        Ok(sinks)
    }
}

#[derive(Debug)]
pub struct CsvCopyToSink {
    encoder: CsvEncoder,
    sink: Box<dyn FileSink>,
}

impl CsvCopyToSink {
    async fn push_inner(&mut self, batch: Batch) -> Result<()> {
        let mut buf = Vec::with_capacity(1024);
        self.encoder.encode(&batch, &mut buf)?;
        self.sink.write_all(buf.into()).await?;

        Ok(())
    }

    async fn finalize_inner(&mut self) -> Result<()> {
        self.sink.finish().await?;
        Ok(())
    }
}

impl PartitionSink for CsvCopyToSink {
    fn push2(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
        self.push_inner(batch).boxed()
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        self.finalize_inner().boxed()
    }
}
