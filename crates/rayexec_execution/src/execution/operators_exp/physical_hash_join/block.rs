use std::sync::atomic::{AtomicU32, Ordering};

use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::buffer_manager::BufferManager;
use crate::arrays::datatype::DataType;
use crate::execution::operators_exp::batch_collection::BatchCollectionBlock;

/// ID generator shared across all build side partitions to produce unique block
/// ids.
#[derive(Debug)]
pub struct BlockIdGen {
    next_val: AtomicU32,
}

impl BlockIdGen {
    pub fn new() -> Self {
        BlockIdGen {
            next_val: AtomicU32::new(1), // A '0' ID indicates no block.
        }
    }

    fn next_id(&self) -> u32 {
        self.next_val.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct HashedBlockCollection<B: BufferManager> {
    blocks: Vec<HashedBlock<B>>,
}

impl<B> HashedBlockCollection<B>
where
    B: BufferManager,
{
    pub fn push_batch(&mut self, manager: &B, datatypes: &[DataType], batch: &mut Batch<B>) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct HashedBlock<B: BufferManager> {
    pub id: u32,
    pub hashes: Vec<u64>,
    pub block: BatchCollectionBlock<B>,
}

impl<B> HashedBlock<B>
where
    B: BufferManager,
{
    pub fn new(manager: &B, id_gen: &BlockIdGen, datatypes: &[DataType], capacity: usize) -> Result<Self> {
        let block = BatchCollectionBlock::new(manager, datatypes, capacity)?;
        let hashes = vec![0; capacity]; // TODO: Track

        let id = id_gen.next_id();

        Ok(HashedBlock { id, hashes, block })
    }
}
