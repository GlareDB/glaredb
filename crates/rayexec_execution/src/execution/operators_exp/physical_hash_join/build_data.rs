use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::buffer_manager::BufferManager;
use crate::arrays::datatype::DataType;
use crate::execution::operators_exp::batch_collection::BatchCollectionBlock;

#[derive(Debug)]
pub struct BuildData<B: BufferManager> {
    capacity_per_block: usize,
    blocks: Vec<BuildBlock<B>>,
}

impl<B> BuildData<B>
where
    B: BufferManager,
{
    pub fn push_batch(&mut self, manager: &B, input_types: &[DataType], batch: &Batch<B>) -> Result<()> {
        let mut block = self.pop_or_allocate_block(manager, input_types, batch.num_rows())?;

        // TODO: Hashes

        block.block.append_batch_data(batch)?;

        self.blocks.push(block);

        Ok(())
    }

    fn pop_or_allocate_block(&mut self, manager: &B, input_types: &[DataType], count: usize) -> Result<BuildBlock<B>> {
        debug_assert!(count <= self.capacity_per_block);

        if let Some(last) = self.blocks.last() {
            if last.block.has_capacity_for_rows(count) {
                return Ok(self.blocks.pop().unwrap());
            }
        }

        let block = BuildBlock::new(manager, input_types, self.capacity_per_block)?;

        Ok(block)
    }
}

#[derive(Debug)]
pub struct BuildBlock<B: BufferManager> {
    block: BatchCollectionBlock<B>,
    /// Row hashes, allocated to capacity of the batch block.
    hashes: Vec<u64>,
}

impl<B> BuildBlock<B>
where
    B: BufferManager,
{
    pub fn new(manager: &B, input_types: &[DataType], capacity: usize) -> Result<Self> {
        let block = BatchCollectionBlock::new(manager, input_types, capacity)?;
        let hashes = vec![0; capacity]; // TODO: Track

        Ok(BuildBlock { block, hashes })
    }
}
