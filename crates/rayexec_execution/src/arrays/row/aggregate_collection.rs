use super::aggregate_layout::AggregateLayout;
use super::block::ValidityInitializer;
use super::row_blocks::RowBlocks;
use crate::arrays::array::buffer_manager::NopBufferManager;

/// Collects grouped aggregate data using a row layout.
#[derive(Debug)]
pub struct AggregateCollection {
    layout: AggregateLayout,
    blocks: RowBlocks<NopBufferManager, ValidityInitializer>,
}

impl AggregateCollection {
    pub fn new(layout: AggregateLayout, block_capacity: usize) -> Self {
        unimplemented!()
    }
}
