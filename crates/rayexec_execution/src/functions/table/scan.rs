use std::fmt::Debug;

use dyn_clone::DynClone;
use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;

use crate::storage::table_storage::Projections;

pub trait TableScanFunction: Debug + Sync + Send + DynClone {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn TableScanState>>>;
}

pub trait TableScanState: Debug + Sync + Send {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>>;
}

impl Clone for Box<dyn TableScanFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

/// Helper for wrapping an unprojected scan with a projections list to produce
/// projected batches.
///
/// This is inefficient compared to handling the projection in the scan itself
/// since this projects a batch after it's already been read.
#[derive(Debug)]
pub struct ProjectedTableScanState<S> {
    pub projections: Projections,
    pub scan_state: S,
}

impl<S: TableScanState> ProjectedTableScanState<S> {
    pub fn new(scan_state: S, projections: Projections) -> Self {
        ProjectedTableScanState {
            projections,
            scan_state,
        }
    }

    async fn pull_inner(&mut self) -> Result<Option<Batch>> {
        let batch = match self.scan_state.pull().await? {
            Some(batch) => batch,
            None => return Ok(None),
        };

        match self.projections.column_indices.as_ref() {
            Some(indices) => {
                let batch = batch.project(indices);
                Ok(Some(batch))
            }
            None => Ok(Some(batch)),
        }
    }
}

impl<S: TableScanState> TableScanState for ProjectedTableScanState<S> {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async { self.pull_inner().await })
    }
}
