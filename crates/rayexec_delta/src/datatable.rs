use futures::stream::{BoxStream, StreamExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use rayexec_execution::{
    database::table::{DataTable, DataTableScan},
    execution::operators::PollPull,
};
use std::sync::Arc;
use std::{
    fmt,
    task::{Context, Poll},
};

use crate::protocol::table::Table;

#[derive(Debug)]
pub struct DeltaDataTable {
    pub table: Arc<Table>,
}

impl DataTable for DeltaDataTable {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let table_scans = self.table.scan(num_partitions)?;
        let scans: Vec<_> = table_scans
            .into_iter()
            .map(|scan| {
                Box::new(DeltaTableScan {
                    stream: scan.into_stream(),
                }) as _
            })
            .collect();

        Ok(scans)
    }
}

struct DeltaTableScan {
    stream: BoxStream<'static, Result<Batch>>,
}

impl DataTableScan for DeltaTableScan {
    fn poll_pull(&mut self, cx: &mut Context) -> Result<PollPull> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Ok(PollPull::Batch(batch)),
            Poll::Ready(Some(Err(e))) => Err(e),
            Poll::Ready(None) => Ok(PollPull::Exhausted),
            Poll::Pending => Ok(PollPull::Pending),
        }
    }
}

impl fmt::Debug for DeltaTableScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowGroupsScan").finish_non_exhaustive()
    }
}
