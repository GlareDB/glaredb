use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::expr::physical::PhysicalSortExpression;

#[derive(Debug)]
pub struct SortPartitionState {
    /// Selection indices that would produce a sorted batch.
    selection: Vec<usize>,
}

impl SortPartitionState {
    pub fn push_local_batch(
        &mut self,
        exprs: &[PhysicalSortExpression],
        batch: &mut Batch,
    ) -> Result<()> {
        unimplemented!()
    }
}
