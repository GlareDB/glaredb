use rayexec_error::Result;

use super::block::{NopInitializer, RowLayoutBlockInitializer};
use super::row_blocks::{BlockAppendState, RowBlocks};
use super::row_layout::RowLayout;
use super::sort_layout::SortLayout;
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::Array;

#[derive(Debug)]
pub struct SortedRowAppendState {
    /// State for appending to row blocks for sort keys.
    key_block_append: BlockAppendState,
    /// State for appending row/heap blocks for columns in the sort key that
    /// need heap blocks.
    key_heap_block_append: BlockAppendState,
    /// State for appending to row blocks for data not part of the sort key.
    data_block_append: BlockAppendState,
    /// Reusable buffer for computing heaps sizes needed per row.
    ///
    /// This used for computing the heap sizes for sort key data and non-sort
    /// key data.
    heap_sizes: Vec<usize>,
}

#[derive(Debug)]
pub struct SortedRowCollection {
    /// Layout for the sorting keys.
    key_layout: SortLayout,
    /// layout for data that's not part of the sorting key.
    data_layout: RowLayout,
    /// Storage for keys.
    ///
    /// No block initialization is needed. This should also never allocate heap
    /// blocks.
    key_blocks: RowBlocks<NopBufferManager, NopInitializer>,
    /// Storage for keys that require heap blocks (varlen, nested).
    ///
    /// In addition to having prefixs encoded in `key_blocks`, varlen keys are
    /// also fully encoded here using the normal row layout.
    ///
    /// By splitting the full encoding out, we can working fixed sized blocks
    /// when comparing keys.
    key_heap_blocks: RowBlocks<NopBufferManager, RowLayoutBlockInitializer>,
    /// Storage for data not part of the keys.
    data_blocks: RowBlocks<NopBufferManager, RowLayoutBlockInitializer>,
}

impl SortedRowCollection {
    pub fn append_keys_and_data(
        &mut self,
        state: &mut SortedRowAppendState,
        keys: &[Array],
        data: &[Array],
    ) -> Result<()> {
        debug_assert_eq!(keys.len(), self.key_layout.columns.len());
        debug_assert_eq!(data.len(), self.data_layout.num_columns());

        unimplemented!()
    }
}
