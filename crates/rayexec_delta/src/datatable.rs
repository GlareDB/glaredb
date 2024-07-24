use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use rayexec_execution::database::table::{DataTable, DataTableScan};
use std::sync::Arc;

use crate::protocol::table::{Table, TableScan};

#[derive(Debug)]
pub struct DeltaDataTable {
    pub table: Arc<Table>,
}

impl DataTable for DeltaDataTable {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let table_scans = self.table.scan(num_partitions)?;
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
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async { self.scan.read_next().await })
    }
}
