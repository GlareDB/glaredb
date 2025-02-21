use std::borrow::Borrow;

use crate::buffer::buffer_manager::NopBufferManager;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::row::block::Block;
use crate::arrays::row::block_scan::BlockScanState;
use crate::arrays::row::row_collection::RowCollection;
use crate::arrays::row::row_layout::RowLayout;
use crate::arrays::sort::partial_sort::PartialSortedRowCollection;
use crate::arrays::sort::sort_layout::{SortColumn, SortLayout};
use crate::arrays::sort::sorted_block::SortedBlock;

/// Wrapper around arrays that have been encoded to a single row block and some
/// number of heap blocks.
#[derive(Debug)]
pub struct TestRowBlock {
    pub layout: RowLayout,
    pub rows: Block<NopBufferManager>,
    pub heap: Vec<Block<NopBufferManager>>,
}

impl TestRowBlock {
    /// Create a TestBlock from a batch.
    ///
    /// Internally generates the blocks using `RowCollection`.
    #[track_caller]
    pub fn from_batch(batch: &Batch) -> Self {
        Self::from_arrays(&batch.arrays, batch.num_rows())
    }

    #[track_caller]
    pub fn from_arrays<A>(arrays: &[A], num_rows: usize) -> Self
    where
        A: Borrow<Array>,
    {
        let layout = RowLayout::new(arrays.iter().map(|a| a.borrow().datatype.clone()));
        let mut collection = RowCollection::new(layout.clone(), num_rows);

        let mut state = collection.init_append();
        collection
            .append_arrays(&mut state, arrays, num_rows)
            .unwrap();

        let (mut row_blocks, mut heap_blocks) = collection.blocks_mut().take_blocks();
        assert_eq!(1, row_blocks.len());

        let rows = row_blocks.pop().unwrap();

        TestRowBlock {
            layout,
            rows,
            heap: heap_blocks,
        }
    }

    #[track_caller]
    pub fn scan_batch(&self) -> Batch {
        let cap = self.rows.num_rows(self.layout.row_width);
        let mut batch = Batch::new(self.layout.types.clone(), cap).unwrap();

        let mut state = BlockScanState::empty();
        unsafe {
            state.prepare_block_scan(&self.rows, self.layout.row_width, 0..cap, true);
            self.layout
                .read_arrays(
                    state.row_pointers_iter(),
                    batch.arrays.iter_mut().enumerate(),
                    0,
                )
                .unwrap();
        }
        batch.set_num_rows(cap).unwrap();

        batch
    }
}

#[derive(Debug)]
pub struct TestSortedRowBlock {
    pub key_layout: SortLayout,
    pub data_layout: RowLayout,
    pub sorted_block: SortedBlock<NopBufferManager>,
}

impl TestSortedRowBlock {
    /// Creates a sorted row block from the given keys and data.
    ///
    /// Key arrays will be ordered ASC and NULLS LAST.
    ///
    /// Internally uses `PartialSortedRowCollection` for generating the blocks.
    #[track_caller]
    pub fn from_arrays<A>(keys: &[A], data: &[A], num_rows: usize) -> Self
    where
        A: Borrow<Array>,
    {
        let key_layout = SortLayout::new(keys.iter().map(|arr| SortColumn {
            desc: false,
            nulls_first: false,
            datatype: arr.borrow().datatype().clone(),
        }));
        let data_layout = RowLayout::new(data.iter().map(|arr| arr.borrow().datatype().clone()));

        let mut collection =
            PartialSortedRowCollection::new(key_layout.clone(), data_layout.clone(), num_rows);
        let mut state = collection.init_append_state();

        collection
            .append_unsorted_keys_and_data(&mut state, keys, data, num_rows)
            .unwrap();
        collection.sort_unsorted();

        let mut blocks = collection.try_into_sorted_blocks().unwrap();
        assert_eq!(1, blocks.len());

        let block = blocks.pop().unwrap();

        TestSortedRowBlock {
            key_layout,
            data_layout,
            sorted_block: block,
        }
    }

    /// Create a sorted row block from a batch with `keys` containing indices to
    /// use for the sort keys.
    pub fn from_batch(batch: &Batch, keys: impl IntoIterator<Item = usize>) -> Self {
        let keys: Vec<_> = keys.into_iter().map(|idx| &batch.arrays[idx]).collect();
        // This is just for typing, `from_arrays` takes a single `A` generic, so
        // we need `&Array`.
        let data: Vec<_> = batch.arrays.iter().map(|arr| arr).collect();

        Self::from_arrays(&keys, &data, batch.num_rows())
    }

    /// Scans the data portion of the sorted row block.
    ///
    /// This will return a batch that's been sorted by the configured keys,
    /// which may or may not be in the batch.
    #[track_caller]
    pub fn scan_data_batch(&self) -> Batch {
        let cap = self.sorted_block.keys.num_rows(self.key_layout.row_width);
        let mut batch = Batch::new(self.data_layout.types.clone(), cap).unwrap();

        let mut state = BlockScanState::empty();
        unsafe {
            state.prepare_block_scan(
                &self.sorted_block.data,
                self.data_layout.row_width,
                0..cap,
                true,
            );
            self.data_layout
                .read_arrays(
                    state.row_pointers_iter(),
                    batch.arrays.iter_mut().enumerate(),
                    0,
                )
                .unwrap();
        }
        batch.set_num_rows(cap).unwrap();

        batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::arrays::{assert_batches_eq, generate_batch};

    #[test]
    fn test_block_scan() {
        let batch = generate_batch!([1, 2, 3], ["a", "b", "c"]);
        let block = TestRowBlock::from_batch(&batch);
        let got = block.scan_batch();

        assert_batches_eq(&batch, &got);
    }

    #[test]
    fn test_sorted_block_data_scan() {
        let batch = generate_batch!([2, 1, 3], ["b", "a", "c"]);
        let block = TestSortedRowBlock::from_batch(&batch, [0]);
        let got = block.scan_data_batch();

        let expected = generate_batch!([1, 2, 3], ["a", "b", "c"]);
        assert_batches_eq(&expected, &got);
    }
}
