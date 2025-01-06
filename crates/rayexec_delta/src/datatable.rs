use std::sync::Arc;

use futures::future::BoxFuture;
use rayexec_error::Result;
use rayexec_execution::arrays::batch::Batch2;
use rayexec_execution::storage::table_storage::{DataTable, DataTableScan, Projections};

use crate::protocol::table::{Table, TableScan};

#[derive(Debug)]
pub struct DeltaDataTable {
    pub table: Arc<Table>,
}

impl DataTable for DeltaDataTable {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>> {
        let table_scans = self.table.scan(projections, num_partitions)?;
        let scans: Vec<_> = table_scans
            .into_iter()
            .map(|scan| Box::new(DeltaTableScan { scan }) as _)
            .collect();

        Ok(scans)
    }
}

#[derive(Debug)]
struct DeltaTableScan {
    scan: TableScan,
}

impl DataTableScan for DeltaTableScan {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch2>>> {
        Box::pin(async { self.scan.read_next().await })
    }
}
