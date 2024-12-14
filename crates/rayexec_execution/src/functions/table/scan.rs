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
