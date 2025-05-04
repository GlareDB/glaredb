use std::borrow::Borrow;

use glaredb_error::Result;

use super::key_encode::KeyEncodeAppendState;
use super::sort_layout::SortLayout;
use super::sorted_block::SortedBlock;
use crate::arrays::array::Array;
use crate::arrays::row::block::{Block, NopInitializer, ValidityInitializer};
use crate::arrays::row::row_blocks::{BlockAppendState, RowBlocks};
use crate::arrays::row::row_layout::RowLayout;
use crate::arrays::sort::key_encode::sort_key_encode;
use crate::buffer::buffer_manager::DefaultBufferManager;

#[derive(Debug)]
pub struct SortedRowAppendState {
    /// State for encoding and appending keys.
    key_append_state: KeyEncodeAppendState,
    /// State for appending to row blocks for data not part of the sort key.
    data_block_append: BlockAppendState,
    /// Reusable buffer for computing heaps sizes needed per row.
    data_heap_sizes: Vec<usize>,
}

/// A collection of partially sorted rows.
///
/// As rows are appended, they get incrementally sorted such that a "block" in
/// the collection is sorted, but blocks are not totally sorted amongst
/// themselves.
// TODO: Chunky (608 bytes)
#[derive(Debug)]
pub struct PartialSortedRowCollection {
    /// Layout for the sorting keys.
    key_layout: SortLayout,
    /// layout for data that's not part of the sorting key.
    data_layout: RowLayout,
    /// Storage for keys.
    ///
    /// No block initialization is needed. This should also never allocate heap
    /// blocks.
    key_blocks: RowBlocks<NopInitializer>,
    /// Storage for keys that require heap blocks (varlen, nested).
    ///
    /// In addition to having prefixs encoded in `key_blocks`, varlen keys are
    /// also fully encoded here using the normal row layout.
    ///
    /// By splitting the full encoding out, we can working fixed sized blocks
    /// when comparing keys.
    key_heap_blocks: RowBlocks<ValidityInitializer>,
    /// Storage for data not part of the keys.
    data_blocks: RowBlocks<ValidityInitializer>,
    /// All blocks sorted so far.
    sorted: Vec<SortedBlock>,
}

impl PartialSortedRowCollection {
    pub fn new(key_layout: SortLayout, data_layout: RowLayout, block_capacity: usize) -> Self {
        let key_blocks = RowBlocks::new(
            &DefaultBufferManager,
            NopInitializer,
            key_layout.row_width,
            block_capacity,
            None,
        );

        let key_heap_blocks = RowBlocks::new_using_row_layout(
            &DefaultBufferManager,
            &key_layout.heap_layout,
            block_capacity,
        );

        let data_blocks =
            RowBlocks::new_using_row_layout(&DefaultBufferManager, &data_layout, block_capacity);

        PartialSortedRowCollection {
            key_layout,
            data_layout,
            key_blocks,
            key_heap_blocks,
            data_blocks,
            sorted: Vec::new(),
        }
    }

    pub fn unsorted_row_count(&self) -> usize {
        self.key_blocks.reserved_row_count()
    }

    pub fn sorted_row_count(&self) -> usize {
        self.sorted
            .iter()
            .map(|block| block.keys.num_rows(self.key_layout.row_width))
            .sum()
    }

    pub fn init_append_state(&self) -> SortedRowAppendState {
        // TODO: We should probably be able to reuse the same append state for
        // each step.
        SortedRowAppendState {
            key_append_state: KeyEncodeAppendState::empty(),
            data_block_append: BlockAppendState {
                row_pointers: Vec::new(),
                heap_pointers: Vec::new(),
            },
            data_heap_sizes: Vec::new(),
        }
    }

    pub fn append_unsorted_keys_and_data<A>(
        &mut self,
        state: &mut SortedRowAppendState,
        keys: &[A],
        data: &[A],
        count: usize,
    ) -> Result<()>
    where
        A: Borrow<Array>,
    {
        debug_assert_eq!(keys.len(), self.key_layout.columns.len());
        debug_assert_eq!(data.len(), self.data_layout.num_columns());

        // Encode keys...
        sort_key_encode(
            &self.key_layout,
            &mut state.key_append_state,
            &mut self.key_blocks,
            &mut self.key_heap_blocks,
            keys,
            0..count,
        )?;

        // Now encode data not part of the key.
        if self.data_layout.num_columns() > 0 {
            // TODO: Only compute heap sizes if heap is actually needed.
            state.data_heap_sizes.resize(count, 0);
            self.data_layout
                .compute_heap_sizes(data, 0..count, &mut state.data_heap_sizes)?;
            state.data_block_append.clear();
            self.data_blocks.prepare_append(
                &mut state.data_block_append,
                count,
                Some(&state.data_heap_sizes),
            )?;
            unsafe {
                self.data_layout
                    .write_arrays(&mut state.data_block_append, data, 0..count)?;
            }
        }

        Ok(())
    }

    /// Sorts all currently unsorted data that we've collected.
    ///
    /// Note that this may produce sorted blocks that exceed our initial block
    /// capacity (e.g. may produce a block of 2117 rows when we have a capacity
    /// of 2048). This is for implementation simplicity.
    ///
    /// An optional limit hint may be provided which will apply a limit to the
    /// resulting sorted block. Note that the total number of sorted rows across
    /// all sorted blocks may exceed the limit as there's not yet any ordering
    /// guarantees between the blocks.
    pub fn sort_unsorted(&mut self, limit_hint: Option<usize>) -> Result<()> {
        let (keys, _) = self.key_blocks.take_blocks(); // Keys should never have heap blocks.
        let (heap_keys, heap_keys_heap) = self.key_heap_blocks.take_blocks();
        let (data, data_heap) = self.data_blocks.take_blocks();

        // Concat all fixed sized blocks into one.
        //
        // Note we don't concat heap blocks as we have active pointers to them
        // in the fixed sized blocks.
        //
        // TODO: Try to avoid concatenating here. We currently do it for ease of
        // implmentation when building the sorted block.
        let keys = Block::concat(&DefaultBufferManager, keys)?;
        let heap_keys = Block::concat(&DefaultBufferManager, heap_keys)?;
        let data = Block::concat(&DefaultBufferManager, data)?;

        let sorted_block = SortedBlock::sort_from_blocks(
            &DefaultBufferManager,
            &self.key_layout,
            &self.data_layout,
            keys,
            heap_keys,
            heap_keys_heap,
            data,
            data_heap,
            limit_hint,
        )?;

        if let Some(block) = sorted_block {
            self.sorted.push(block);
        }

        Ok(())
    }

    /// Try sort any remaining unsorted blocks, and return all sorted blocks in
    /// the collection.
    pub fn try_into_sorted_blocks(mut self, limit_hint: Option<usize>) -> Result<Vec<SortedBlock>> {
        self.sort_unsorted(limit_hint)?;
        debug_assert_eq!(0, self.unsorted_row_count());

        Ok(self.sorted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::arrays::row::block_scan::BlockScanState;
    use crate::arrays::sort::sort_layout::SortColumn;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    /// Helper for asserting that key/data inputs sort correctly with the
    /// resulting data matching `expected`.
    ///
    /// This will create sorted row collection with a capacity for 16 for each
    /// block. The keys/data will immediately be inserted then sorted. We assume
    /// this produces a single sorted block, and scan the data from that to use
    /// in the assertions.
    fn assert_sort_as_expected<A>(
        key_layout: SortLayout,
        data_layout: RowLayout,
        keys: &[A],
        data: &[A],
        expected_sort_data: &[Array],
    ) where
        A: Borrow<Array>,
    {
        let mut collection = PartialSortedRowCollection::new(key_layout, data_layout, 16);
        let row_count = keys.first().unwrap().borrow().logical_len();
        assert!(row_count <= 16); // For testing purposes.

        let mut state = collection.init_append_state();
        collection
            .append_unsorted_keys_and_data(&mut state, keys, data, row_count)
            .unwrap();

        assert_eq!(row_count, collection.unsorted_row_count());
        assert_eq!(0, collection.sorted_row_count());

        collection.sort_unsorted(None).unwrap();
        assert_eq!(0, collection.unsorted_row_count());
        assert_eq!(row_count, collection.sorted_row_count());

        assert_eq!(1, collection.sorted.len());

        let mut read_state = BlockScanState::empty();
        unsafe {
            collection.sorted[0]
                .prepare_data_read(&mut read_state, &collection.data_layout, 0..row_count)
                .unwrap();
        }

        let mut arrays = collection
            .data_layout
            .types
            .iter()
            .map(|datatype| Array::new(&DefaultBufferManager, datatype.clone(), row_count))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        unsafe {
            collection
                .data_layout
                .read_arrays(
                    read_state.row_pointers_iter(),
                    arrays.iter_mut().enumerate(),
                    0,
                )
                .unwrap();
        }

        assert_eq!(expected_sort_data.len(), arrays.len());
        for (expected, got) in expected_sort_data.iter().zip(&arrays) {
            assert_arrays_eq(expected, got);
        }
    }

    #[test]
    fn sort_single_key_i32_already_sorted() {
        let key_layout = SortLayout::new([SortColumn {
            desc: false,
            nulls_first: false,
            datatype: DataType::Int32,
        }]);
        let data_layout = RowLayout::new([DataType::Int32]);

        let keys = Array::try_from_iter([1, 2, 3]).unwrap();
        let expected = Array::try_from_iter([1, 2, 3]).unwrap();

        assert_sort_as_expected(key_layout, data_layout, &[&keys], &[&keys], &[expected]);
    }

    #[test]
    fn sort_single_key_i32() {
        let key_layout = SortLayout::new([SortColumn {
            desc: false,
            nulls_first: false,
            datatype: DataType::Int32,
        }]);
        let data_layout = RowLayout::new([DataType::Int32]);

        let keys = Array::try_from_iter([2, 3, 1]).unwrap();
        let expected = Array::try_from_iter([1, 2, 3]).unwrap();

        assert_sort_as_expected(key_layout, data_layout, &[&keys], &[&keys], &[expected]);
    }

    #[test]
    fn sort_single_key_utf8_lexical() {
        let key_layout = SortLayout::new([SortColumn::new_asc_nulls_last(DataType::Utf8)]);
        let data_layout = RowLayout::new([DataType::Utf8]);

        let keys = generate_batch!(["1", "2", "10", "20"]);
        let expected = generate_batch!(["1", "10", "2", "20"]);

        assert_sort_as_expected(
            key_layout,
            data_layout,
            &keys.arrays,
            &keys.arrays,
            &expected.arrays,
        );
    }

    #[test]
    fn sort_two_keys_utf8_i32_with_ties() {
        let key_layout = SortLayout::new([
            SortColumn::new_asc_nulls_last(DataType::Utf8),
            SortColumn::new_asc_nulls_last(DataType::Int32),
        ]);
        let data_layout = RowLayout::new([DataType::Utf8, DataType::Int32]);

        let keys = generate_batch!(["a", "b", "b", "c"], [0, 1, 0, 0]);
        let expected = generate_batch!(["a", "b", "b", "c"], [0, 0, 1, 0]);

        assert_sort_as_expected(
            key_layout,
            data_layout,
            &keys.arrays,
            &keys.arrays,
            &expected.arrays,
        );
    }

    #[test]
    fn sort_two_keys_utf8_i32_with_all_ties() {
        let key_layout = SortLayout::new([
            SortColumn::new_asc_nulls_last(DataType::Utf8),
            SortColumn::new_asc_nulls_last(DataType::Int32),
        ]);
        let data_layout = RowLayout::new([DataType::Utf8, DataType::Int32]);

        let keys = generate_batch!(["b", "b", "b", "b"], [3, 1, 2, 4]);
        let expected = generate_batch!(["b", "b", "b", "b"], [1, 2, 3, 4]);

        assert_sort_as_expected(
            key_layout,
            data_layout,
            &keys.arrays,
            &keys.arrays,
            &expected.arrays,
        );
    }

    #[test]
    fn sort_three_keys_utf8_utf8_i32_with_layered_ties() {
        let key_layout = SortLayout::new([
            SortColumn::new_asc_nulls_last(DataType::Utf8),
            SortColumn::new_asc_nulls_last(DataType::Utf8),
            SortColumn::new_asc_nulls_last(DataType::Int32),
        ]);
        let data_layout = RowLayout::new([DataType::Utf8, DataType::Utf8, DataType::Int32]);

        let keys = generate_batch!(["a", "a", "a", "a"], ["b", "c", "b", "c"], [3, 1, 2, 4]);
        let expected = generate_batch!(["a", "a", "a", "a"], ["b", "b", "c", "c"], [2, 3, 1, 4]);

        assert_sort_as_expected(
            key_layout,
            data_layout,
            &keys.arrays,
            &keys.arrays,
            &expected.arrays,
        );
    }

    #[test]
    fn sort_single_key_utf8_all_inline() {
        let key_layout = SortLayout::new([SortColumn {
            desc: false,
            nulls_first: false,
            datatype: DataType::Utf8,
        }]);
        let data_layout = RowLayout::new([DataType::Utf8]);

        let keys = Array::try_from_iter(["a", "c", "b"]).unwrap();
        let expected = Array::try_from_iter(["a", "b", "c"]).unwrap();
        assert_sort_as_expected(key_layout, data_layout, &[&keys], &[&keys], &[expected]);
    }

    #[test]
    fn sort_single_key_utf8_all_inline_desc() {
        let key_layout = SortLayout::new([SortColumn {
            desc: true,
            nulls_first: false,
            datatype: DataType::Utf8,
        }]);
        let data_layout = RowLayout::new([DataType::Utf8]);

        let keys = Array::try_from_iter(["a", "c", "b"]).unwrap();
        let expected = Array::try_from_iter(["c", "b", "a"]).unwrap();
        assert_sort_as_expected(key_layout, data_layout, &[&keys], &[&keys], &[expected]);
    }

    #[test]
    fn sort_multiple_keys_with_ties_i32() {
        // Tie on fixed length column.

        let key_layout = SortLayout::new([
            SortColumn {
                desc: false,
                nulls_first: false,
                datatype: DataType::Int32,
            },
            SortColumn {
                desc: false,
                nulls_first: false,
                datatype: DataType::Int32,
            },
        ]);
        let data_layout = RowLayout::new([DataType::Int32, DataType::Int32]);

        let keys = [
            Array::try_from_iter([2, 2, 1]).unwrap(),
            Array::try_from_iter([6, 5, 4]).unwrap(),
        ];
        let expected = [
            Array::try_from_iter([1, 2, 2]).unwrap(),
            Array::try_from_iter([4, 5, 6]).unwrap(),
        ];

        assert_sort_as_expected(key_layout, data_layout, &keys, &keys, &expected);
    }

    #[test]
    fn sort_multiple_keys_with_ties_desc_i32() {
        // Tie on fixed length column, descending on tied column, asc on second
        // column.

        let key_layout = SortLayout::new([
            SortColumn {
                desc: true,
                nulls_first: false,
                datatype: DataType::Int32,
            },
            SortColumn {
                desc: false,
                nulls_first: false,
                datatype: DataType::Int32,
            },
        ]);
        let data_layout = RowLayout::new([DataType::Int32, DataType::Int32]);

        let keys = [
            Array::try_from_iter([2, 2, 1]).unwrap(),
            Array::try_from_iter([6, 5, 4]).unwrap(),
        ];
        let expected = [
            Array::try_from_iter([2, 2, 1]).unwrap(),
            Array::try_from_iter([5, 6, 4]).unwrap(), // Second column still ascending within tied rows.
        ];

        assert_sort_as_expected(key_layout, data_layout, &keys, &keys, &expected);
    }
}
