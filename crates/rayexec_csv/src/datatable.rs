use std::fmt::{self, Debug};

use futures::future::BoxFuture;
use rayexec_error::Result;
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::table_storage::{
    DataTable,
    DataTableScan,
    EmptyTableScan,
    ProjectedScan,
    Projections,
};
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::FileProvider;

use crate::reader::{AsyncCsvReader, CsvSchema, DialectOptions};

/// Data table implementation that reads from a single file.
///
/// This will produce a single scan that reads the actual file, with the
/// remaining scans being empty.
///
/// This should be extended to support multiple files once we add in glob
/// support.
#[derive(Debug)]
pub struct SingleFileCsvDataTable<R: Runtime> {
    pub options: DialectOptions,
    pub csv_schema: CsvSchema,
    pub location: FileLocation,
    pub conf: AccessConfig,
    pub runtime: R,
}

impl<R: Runtime> DataTable for SingleFileCsvDataTable<R> {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>> {
        let reader = self
            .runtime
            .file_provider()
            .file_source(self.location.clone(), &self.conf)?;
        let csv_reader = AsyncCsvReader::new(reader, self.csv_schema.clone(), self.options);

        let mut scans: Vec<Box<dyn DataTableScan>> = vec![Box::new(ProjectedScan::new(
            CsvFileScan { reader: csv_reader },
            projections,
        ))];
        // Reset are empty (for now)
        scans.extend((1..num_partitions).map(|_| Box::new(EmptyTableScan) as _));

        Ok(scans)
    }
}

pub struct CsvFileScan {
    reader: AsyncCsvReader,
}

impl DataTableScan for CsvFileScan {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async { self.reader.read_next().await })
    }
}

impl fmt::Debug for CsvFileScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvFileScan").finish_non_exhaustive()
    }
}
