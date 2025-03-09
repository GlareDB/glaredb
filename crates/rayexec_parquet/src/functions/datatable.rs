use std::collections::VecDeque;
use std::fmt::{self, Debug};
use std::sync::Arc;

use futures::future::BoxFuture;
use rayexec_error::Result;
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::arrays::field::ColumnSchema;
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::table_storage::{DataTable, DataTableScan, Projections};
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::{FileProvider2, FileSource};

use crate::metadata::Metadata;

/// Data table implementation which parallelizes on row groups. During scanning,
/// each returned scan object is responsible for distinct row groups to read.
#[derive(Debug)]
pub struct RowGroupPartitionedDataTable<R: Runtime> {
    pub metadata: Arc<Metadata>,
    pub schema: ColumnSchema,
    pub location: FileLocation,
    pub conf: AccessConfig,
    pub runtime: R,
}

impl<R: Runtime> DataTable for RowGroupPartitionedDataTable<R> {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>> {
        let file_provider = self.runtime.file_provider();

        let mut partitioned_row_groups = vec![VecDeque::new(); num_partitions];

        // Split row groups into individual partitions.
        for row_group in 0..self.metadata.decoded_metadata.row_groups().len() {
            let partition = row_group % num_partitions;
            partitioned_row_groups[partition].push_back(row_group);
        }

        // let readers = partitioned_row_groups
        //     .into_iter()
        //     .map(|row_groups| {
        //         let reader = file_provider.file_source(self.location.clone(), &self.conf)?;
        //         const BATCH_SIZE: usize = 4096; // TODO
        //         AsyncBatchReader::try_new(
        //             reader,
        //             row_groups,
        //             self.metadata.clone(),
        //             &self.schema,
        //             BATCH_SIZE,
        //             projections.clone(),
        //         )
        //     })
        //     .collect::<Result<Vec<_>>>()?;

        unimplemented!()
        // let scans: Vec<Box<dyn DataTableScan>> = readers
        //     .into_iter()
        //     .map(|reader| Box::new(RowGroupsScan { reader }) as _)
        //     .collect();

        // Ok(scans)
    }
}
