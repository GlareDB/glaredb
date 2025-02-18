use std::pin::Pin;
use std::task::Context;

use rayexec_error::Result;
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::execution::operators::source::operation::{
    PartitionSource,
    PollPull,
    Projections,
    SourceOperation,
};
use rayexec_execution::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use rayexec_execution::runtime::Runtime;
use rayexec_io::exp::AsyncReadStream;
use rayexec_io::location::{AccessConfig, FileLocation};

use crate::dialect::DialectOptions;
use crate::reader::CsvReader;
use crate::schema::CsvSchema;

#[derive(Debug)]
pub struct SingleFileCsvScan {
    pub read_buffer: Vec<u8>,
    pub stream: Pin<Box<dyn AsyncReadStream>>,
    pub options: DialectOptions,
    pub csv_schema: CsvSchema,
}

impl SourceOperation for SingleFileCsvScan {
    fn create_partition_sources(
        &mut self,
        context: &DatabaseContext,
        projections: &Projections,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSource>>> {
        unimplemented!()
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
