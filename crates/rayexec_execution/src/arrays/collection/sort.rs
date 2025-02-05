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

// TODO: Chunky (608 bytes)
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
    pub fn new(key_layout: SortLayout, data_layout: RowLayout, block_capacity: usize) -> Self {
        let key_blocks = RowBlocks::new(
            NopBufferManager,
            NopInitializer,
            key_layout.compare_width,
            block_capacity,
        );

        let key_heap_blocks = RowBlocks::new_using_row_layout(
            NopBufferManager,
            key_layout.heap_layout.clone(),
            block_capacity,
        );

        let data_blocks =
            RowBlocks::new_using_row_layout(NopBufferManager, data_layout.clone(), block_capacity);

        SortedRowCollection {
            key_layout,
            data_layout,
            key_blocks,
            key_heap_blocks,
            data_blocks,
        }
    }

    pub fn unsorted_row_count(&self) -> usize {
        self.key_blocks.reserved_row_count()
    }

    pub fn init_append_state(&self) -> SortedRowAppendState {
        // TODO: We should probably be able to reuse the same append state for
        // each step.
        SortedRowAppendState {
            key_block_append: BlockAppendState {
                row_pointers: Vec::new(),
                heap_pointers: Vec::new(),
            },
            key_heap_block_append: BlockAppendState {
                row_pointers: Vec::new(),
                heap_pointers: Vec::new(),
            },
            data_block_append: BlockAppendState {
                row_pointers: Vec::new(),
                heap_pointers: Vec::new(),
            },
            heap_sizes: Vec::new(),
        }
    }

    pub fn append_unsorted_keys_and_data(
        &mut self,
        state: &mut SortedRowAppendState,
        keys: &[Array],
        data: &[Array],
        count: usize,
    ) -> Result<()> {
        debug_assert_eq!(keys.len(), self.key_layout.columns.len());
        debug_assert_eq!(data.len(), self.data_layout.num_columns());

        // Encode sort keys first (no heap values)
        state.key_block_append.clear();
        self.key_blocks
            .prepare_append(&mut state.key_block_append, count, None)?;
        debug_assert_eq!(0, self.key_blocks.num_heap_blocks());
        unsafe {
            self.key_layout
                .write_key_arrays(&mut state.key_block_append, keys, count)?;
        }

        // Encode sort keys that require heap blocks.
        if self.key_layout.requires_heap() {
            let heap_keys: Vec<_> = self
                .key_layout
                .heap_mapping
                .keys()
                .map(|&idx| &keys[idx])
                .collect();

            // Compute heap sizes needed.
            state.heap_sizes.resize(count, 0);
            self.key_layout.heap_layout.compute_heap_sizes(
                &heap_keys,
                count,
                &mut state.heap_sizes,
            )?;

            state.key_heap_block_append.clear();
            self.key_heap_blocks.prepare_append(
                &mut state.key_heap_block_append,
                count,
                Some(&state.heap_sizes),
            )?;

            unsafe {
                self.key_layout.heap_layout.write_arrays(
                    &mut state.key_heap_block_append,
                    &heap_keys,
                    count,
                )?;
            }
        }

        // Now encode data not part of the key.
        if self.data_layout.num_columns() > 0 {
            state.heap_sizes.resize(count, 0);
            self.data_layout
                .compute_heap_sizes(&data, count, &mut state.heap_sizes)?;
            state.data_block_append.clear();
            self.data_blocks.prepare_append(
                &mut state.data_block_append,
                count,
                Some(&state.heap_sizes),
            )?;
            unsafe {
                self.data_layout
                    .write_arrays(&mut state.data_block_append, data, count)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::collection::sort_layout::SortColumn;
    use crate::arrays::datatype::DataType;

    #[test]
    fn append_single_key_column() {
        let key_layout = SortLayout::new([SortColumn {
            desc: false,
            nulls_first: false,
            datatype: DataType::Int32,
        }]);
        let data_layout = RowLayout::new([]);
        let mut collection = SortedRowCollection::new(key_layout, data_layout, 16);

        let mut state = collection.init_append_state();
        let keys = Array::try_from_iter([1, 2, 3]).unwrap();
        collection
            .append_unsorted_keys_and_data(&mut state, &[keys], &[], 3)
            .unwrap();

        assert_eq!(3, collection.unsorted_row_count());
    }
}
