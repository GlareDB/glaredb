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
use rayexec_io::location::{AccessConfig, FileLocation};

use crate::reader::{AsyncCsvReader, CsvSchema, DialectOptions};

#[derive(Debug)]
pub struct SingleFileCsvScan<R: Runtime> {
    pub options: DialectOptions,
    pub csv_schema: CsvSchema,
    pub location: FileLocation,
    pub conf: AccessConfig,
    pub runtime: R,
}

impl<R> SourceOperation for SingleFileCsvScan<R>
where
    R: Runtime,
{
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

impl<R> Explainable for SingleFileCsvScan<R>
where
    R: Runtime,
{
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CsvFileScan")
    }
}

#[derive(Debug)]
pub struct CsvPartitionScan {
    reader: AsyncCsvReader,
}

impl PartitionSource for CsvPartitionScan {
    fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        unimplemented!()
    }
}
