use std::borrow::{Borrow, BorrowMut};
use std::collections::VecDeque;

use rayexec_error::Result;

use super::row_blocks::{BlockAppendState, BlockReadState, RowBlocks};
use super::row_layout::RowLayout;
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;

/// Address to a single row in the collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowAddress {
    pub chunk_idx: u32,
    pub row_idx: u16,
}

/// State for appending data to the collection.
#[derive(Debug)]
pub struct RowAppendState {
    block_append: BlockAppendState,
    heap_sizes: Vec<usize>,
}

/// State for resumable scanning of the row collection.
#[derive(Debug)]
pub struct RowScanState {
    /// Remaining block indices to scan.
    blocks_to_scan: VecDeque<usize>,
    /// Current block we're scanning.
    ///
    /// If None, we should get the next block to scan.
    current_block: Option<usize>,
    /// Row index of the most recent row scanned within the block.
    row_idx: usize,
    /// State containing the pointers for the most recent scan.
    block_read: BlockReadState,
}

impl RowScanState {
    /// Returns the row pointers for the most recent scan.
    pub fn scanned_row_pointers(&self) -> &[*const u8] {
        &self.block_read.row_pointers
    }
}

/// Collects array data by first row-encoding the data and storing it in raw
/// buffers.
#[derive(Debug)]
pub struct RowCollection {
    blocks: RowBlocks<NopBufferManager>,
}

impl RowCollection {
    pub fn new(layout: RowLayout, block_capacity: usize) -> Self {
        RowCollection {
            blocks: RowBlocks::new(NopBufferManager, layout, block_capacity),
        }
    }

    pub fn layout(&self) -> &RowLayout {
        &self.blocks.layout
    }

    pub fn row_count(&self) -> usize {
        self.blocks.reserved_row_count()
    }

    /// Initializes state for appending rows to this collection.
    pub fn init_append(&self) -> RowAppendState {
        RowAppendState {
            block_append: BlockAppendState {
                row_pointers: Vec::new(),
                heap_pointers: Vec::new(),
            },
            heap_sizes: Vec::new(),
        }
    }

    /// Appends a batch to the collection.
    ///
    /// The array types for this batch should match the types specified in the
    /// row layout.
    pub fn append_batch(&mut self, state: &mut RowAppendState, batch: &Batch) -> Result<()> {
        self.append_arrays(state, &batch.arrays, batch.num_rows)
    }

    /// Internal method for appending arrays to this collection.
    ///
    /// The input arrays must match the row layout for the collection.
    ///
    /// Array capacities must equal or exceed `num_rows`.
    pub(crate) fn append_arrays<A>(
        &mut self,
        state: &mut RowAppendState,
        arrays: &[A],
        num_rows: usize,
    ) -> Result<()>
    where
        A: Borrow<Array>,
    {
        state.block_append.clear();
        if self.layout().requires_heap {
            // Compute heap sizes per row.
            state.heap_sizes.resize(num_rows, 0);
            self.layout()
                .compute_heap_sizes(arrays, num_rows, &mut state.heap_sizes)?;
        }

        if self.layout().requires_heap {
            self.blocks.prepare_append(
                &mut state.block_append,
                num_rows,
                Some(&state.heap_sizes),
            )?
        } else {
            self.blocks
                .prepare_append(&mut state.block_append, num_rows, None)?
        }

        // SAFETY: We assume that the pointers we computed are inbounds with
        // respect the blocks, and that we correctly computed the heap sizes.
        unsafe {
            self.layout()
                .write_arrays(&mut state.block_append, arrays, num_rows)?
        };

        Ok(())
    }

    /// Initialize a scan state for scanning all row blocks in the collection.
    pub fn init_full_scan(&self) -> RowScanState {
        self.init_partial_scan(0..self.blocks.num_row_blocks())
    }

    /// Initialize a scan of only some of the row_blocks.
    pub fn init_partial_scan(
        &self,
        block_indices: impl IntoIterator<Item = usize>,
    ) -> RowScanState {
        let blocks_to_scan: VecDeque<_> = block_indices.into_iter().collect();

        RowScanState {
            blocks_to_scan,
            current_block: None,
            row_idx: 0,
            block_read: BlockReadState {
                row_pointers: Vec::new(),
            },
        }
    }

    /// Scan the collection, writing rows to `output`.
    ///
    /// The number of rows scanned is returned. Zero is returned if either the
    /// output batches capacity is zero, or if there are no more rows to scan
    /// according the scan state.
    ///
    /// This updates the scan state to allow for resuming scans.
    pub fn scan(&self, state: &mut RowScanState, output: &mut Batch) -> Result<usize> {
        let columns: Vec<_> = (0..output.arrays.len()).collect(); // TODO: Try not to do this.
        let count = self.scan_columns(state, &columns, &mut output.arrays, output.capacity)?;
        output.set_num_rows(count)?;

        Ok(count)
    }

    /// Scan a subset of columns into the output arrays.
    ///
    /// `columns` provides which columns to scan.
    ///
    /// `count` indicates the max number of rows to write to the array. This
    /// must be less than or equal to the array capacities.
    pub fn scan_columns<A>(
        &self,
        state: &mut RowScanState,
        columns: &[usize],
        outputs: &mut [A],
        count: usize,
    ) -> Result<usize>
    where
        A: BorrowMut<Array>,
    {
        debug_assert_eq!(columns.len(), outputs.len());

        state.block_read.clear();

        let mut scanned_count = 0;
        let mut remaining_cap = count;

        while remaining_cap > 0 {
            // Get the current block to scan.
            //
            // If None, we try to get the next block from the queue and reset
            // the row idx to 0 so that we start scanning that block from the
            // state.
            let current_block = match state.current_block {
                Some(curr) => curr,
                None => {
                    state.row_idx = 0;
                    let new_block = match state.blocks_to_scan.pop_front() {
                        Some(block) => block,
                        None => break,
                    };
                    state.current_block = Some(new_block);
                    state.current_block.unwrap()
                }
            };

            let num_rows = self.blocks.rows_in_row_block(current_block);
            if state.row_idx >= num_rows {
                // No more rows to scan in this chunk, move to next chunk.
                state.current_block = None;
                continue;
            }

            let scan_count = usize::min(remaining_cap, num_rows - state.row_idx);
            self.blocks.prepare_read(
                &mut state.block_read,
                current_block,
                state.row_idx..(state.row_idx + scan_count),
            )?;

            unsafe {
                self.layout().read_arrays(
                    &state.block_read,
                    columns.iter().copied().zip(outputs.iter_mut()),
                    scanned_count,
                    &self.blocks,
                )?;
            }

            // Update state.
            state.row_idx += scan_count;
            remaining_cap -= scan_count;
            scanned_count += scan_count;
        }

        Ok(scanned_count)
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::{assert_arrays_eq, assert_batches_eq, generate_batch};

    #[test]
    fn append_single_batch_i32() {
        let mut collection = RowCollection::new(RowLayout::new([DataType::Int32]), 16);
        let input = generate_batch!([1, 2, 3, 4, 5, 6]);
        let mut state = collection.init_append();
        collection.append_batch(&mut state, &input).unwrap();

        let mut output = Batch::try_new([DataType::Int32], 16).unwrap();

        let mut state = collection.init_full_scan();
        collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(6, state.scanned_row_pointers().len());

        let expected = generate_batch!([1, 2, 3, 4, 5, 6]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn append_single_batch_i32_with_invalid() {
        let mut collection = RowCollection::new(RowLayout::new([DataType::Int32]), 16);
        let input = generate_batch!([Some(1), Some(2), None, Some(4), None, Some(6)]);
        let mut state = collection.init_append();
        collection.append_batch(&mut state, &input).unwrap();
        assert_eq!(6, collection.row_count());

        let mut output = Batch::try_new([DataType::Int32], 16).unwrap();

        let mut state = collection.init_full_scan();
        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(6, scan_count);
        assert_eq!(6, state.scanned_row_pointers().len());

        let expected = generate_batch!([Some(1), Some(2), None, Some(4), None, Some(6)]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn append_multiple_batches_i32_with_invalid() {
        let mut collection = RowCollection::new(RowLayout::new([DataType::Int32]), 16);
        let mut state = collection.init_append();
        let input1 = generate_batch!(0..16);
        collection.append_batch(&mut state, &input1).unwrap();
        let input2 = generate_batch!(16..32);
        collection.append_batch(&mut state, &input2).unwrap();
        assert_eq!(32, collection.row_count());

        let mut output = Batch::try_new([DataType::Int32], 16).unwrap();
        let mut state = collection.init_full_scan();

        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(16, scan_count);
        assert_eq!(16, state.scanned_row_pointers().len());

        let expected1 = generate_batch!(0..16);
        assert_batches_eq(&expected1, &output);

        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(16, scan_count);
        assert_eq!(16, state.scanned_row_pointers().len());

        let expected2 = generate_batch!(16..32);
        assert_batches_eq(&expected2, &output);
    }

    #[test]
    fn append_batch_scan_column_subset() {
        let mut collection =
            RowCollection::new(RowLayout::new([DataType::Int32, DataType::Utf8]), 16);
        let mut state = collection.init_append();
        let input = generate_batch!([1, 2, 3, 4], ["a", "b", "c", "d"]);
        collection.append_batch(&mut state, &input).unwrap();

        // Scan just the string column.
        let mut output = Array::try_new(&NopBufferManager, DataType::Utf8, 4).unwrap();

        let mut state = collection.init_full_scan();
        collection
            .scan_columns(&mut state, &[1], &mut [&mut output], 4)
            .unwrap();

        let expected = Array::try_from_iter(["a", "b", "c", "d"]).unwrap();
        assert_arrays_eq(&expected, &output);
    }

    #[test]
    fn append_batch_scan_no_chunks() {
        let mut collection =
            RowCollection::new(RowLayout::new([DataType::Int32, DataType::Utf8]), 16);
        let mut state = collection.init_append();
        let input = generate_batch!([1, 2, 3, 4], ["a", "b", "c", "d"]);
        collection.append_batch(&mut state, &input).unwrap();

        // Dummy output, nothing should be written.
        let mut output = Array::try_new(&NopBufferManager, DataType::Utf8, 4).unwrap();

        let mut state = collection.init_partial_scan([]);
        let count = collection
            .scan_columns(&mut state, &[1], &mut [&mut output], 4)
            .unwrap();
        assert_eq!(0, count);
    }

    #[test]
    fn append_multiple_batches_scan_single_chunk() {
        let mut collection =
            RowCollection::new(RowLayout::new([DataType::Int32, DataType::Utf8]), 4);
        let mut state = collection.init_append();

        let input1 = generate_batch!([1, 2, 3, 4], ["a", "b", "c", "d"]);
        collection.append_batch(&mut state, &input1).unwrap();
        let input2 = generate_batch!([5, 6, 7, 8], ["e", "f", "g", "h"]);
        collection.append_batch(&mut state, &input2).unwrap();
        let input3 = generate_batch!([9, 10, 11, 12], ["i", "j", "k", "l"]);
        collection.append_batch(&mut state, &input3).unwrap();

        let mut output = Array::try_new(&NopBufferManager, DataType::Utf8, 4).unwrap();

        let mut state = collection.init_partial_scan([1]);
        let count = collection
            .scan_columns(&mut state, &[1], &mut [&mut output], 4)
            .unwrap();
        assert_eq!(4, count);

        let expected = Array::try_from_iter(["e", "f", "g", "h"]).unwrap();
        assert_arrays_eq(&expected, &output);

        let count = collection
            .scan_columns(&mut state, &[1], &mut [&mut output], 4)
            .unwrap();
        assert_eq!(0, count);
    }
}
