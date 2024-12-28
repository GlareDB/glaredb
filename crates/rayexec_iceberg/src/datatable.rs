use std::sync::Arc;

use futures::future::BoxFuture;
use rayexec_error::Result;
use rayexec_execution::arrays::batch::Batch2;
use rayexec_execution::storage::table_storage::{DataTable, DataTableScan, Projections};

use crate::table::{Table, TableScan};

#[derive(Debug)]
pub struct IcebergDataTable {
    pub table: Arc<Table>,
}

impl DataTable for IcebergDataTable {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>> {
        let scans = self.table.scan(projections, num_partitions)?;
        let scans: Vec<_> = scans
            .into_iter()
            .map(|scan| Box::new(IcebergTableScan { scan }) as _)
            .collect();

        Ok(scans)
    }
}

#[derive(Debug)]
struct IcebergTableScan {
    scan: TableScan,
}

impl DataTableScan for IcebergTableScan {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch2>>> {
        Box::pin(async { self.scan.read_next().await })
    }
}
