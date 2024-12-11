use std::fmt::Debug;

use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;

use crate::storage::table_storage::Projections;

pub trait TableOutFunction: Debug + Sync + Send {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn TableOutState>>>;
}

pub trait TableOutState: Debug + Sync + Send {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>>;
}
