use rayexec_error::Result;
use rayexec_execution::arrays::field::ColumnSchema;
use rayexec_execution::execution::operators::sink::operation::PartitionSink;
use rayexec_execution::functions::copy::CopyToFunction;
use rayexec_execution::runtime::Runtime;
use rayexec_io::location::FileLocation;

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
        _schema: ColumnSchema,
        _location: FileLocation,
        _num_partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        unimplemented!()
    }
}
