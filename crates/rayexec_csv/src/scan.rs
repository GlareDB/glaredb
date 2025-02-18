use std::pin::Pin;
use std::task::Context;

use rayexec_error::{RayexecError, Result};
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::execution::operators::source::operation::{
    PartitionSource,
    PollPull,
    Projections,
    SourceOperation,
};
use rayexec_execution::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use rayexec_io::exp::AsyncReadStream;

use crate::decoder::{ByteRecords, CsvDecoder};
use crate::reader::CsvReader;

#[derive(Debug)]
pub struct SingleFileCsvScan {
    pub inner: Option<FileScan>,
}

#[derive(Debug)]
pub struct FileScan {
    pub stream: Pin<Box<dyn AsyncReadStream>>,
    pub skip_header: bool,
    pub read_buffer: Vec<u8>,
    pub decoder: CsvDecoder,
    pub output: ByteRecords,
}

impl SourceOperation for SingleFileCsvScan {
    fn create_partition_sources(
        &mut self,
        _context: &DatabaseContext,
        projections: &Projections,
        _batch_size: usize,
        _partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSource>>> {
        let inner = self
            .inner
            .take()
            .ok_or_else(|| RayexecError::new("Scan called more than once"))?;

        let reader = CsvReader::new(
            inner.stream,
            inner.skip_header,
            projections.clone(),
            inner.read_buffer,
            inner.decoder,
            inner.output,
        );

        Ok(vec![Box::new(CsvPartitionScan { reader })])
    }
}

impl Explainable for SingleFileCsvScan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CsvFileScan")
    }
}

#[derive(Debug)]
pub struct CsvPartitionScan {
    reader: CsvReader,
}

impl PartitionSource for CsvPartitionScan {
    fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        self.reader.poll_pull(cx, output)
    }
}
