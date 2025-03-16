use glaredb_error::Result;

use super::sorted_block::SortedBlock;
use crate::arrays::batch::Batch;
use crate::arrays::row::block::Block;
use crate::arrays::row::block_scan::BlockScanState;
use crate::arrays::row::row_layout::RowLayout;

/// Scan state for scanning a sorted run.
#[derive(Debug)]
pub struct SortedSegmentScanState {
    block_idx: usize,
    row_idx: usize,
    block_scan: BlockScanState,
}

/// Contains multiple blocks that have been totally sorted.
#[derive(Debug)]
pub struct SortedSegment {
    pub(crate) keys: Vec<Block>,
    pub(crate) heap_keys: Vec<Block>,
    pub(crate) heap_keys_heap: Vec<Block>,
    pub(crate) data: Vec<Block>,
    pub(crate) data_heap: Vec<Block>,
}

impl SortedSegment {
    /// Create a new sorted run from a sorted block.
    pub fn from_sorted_block(block: SortedBlock) -> Self {
        SortedSegment {
            keys: vec![block.keys],
            heap_keys: vec![block.heap_keys],
            heap_keys_heap: block.heap_keys_heap,
            data: vec![block.data],
            data_heap: block.data_heap,
        }
    }

    pub fn init_scan_state(&self) -> SortedSegmentScanState {
        SortedSegmentScanState {
            block_idx: 0,
            row_idx: 0,
            block_scan: BlockScanState::empty(),
        }
    }

    /// Scans data from the sorted run into the output batch.
    ///
    /// Returns the number of rows scanned. Zero indicates we've reached the end
    /// of the run.
    pub fn scan_data(
        &self,
        state: &mut SortedSegmentScanState,
        data_layout: &RowLayout,
        output: &mut Batch,
    ) -> Result<usize> {
        let mut count = 0;
        let mut rem_cap = output.write_capacity()?;

        state.block_scan.clear();

        // Get row pointers, possibly from multiple blocks.
        loop {
            if state.block_idx >= self.data.len() || rem_cap == 0 {
                break;
            }

            let block = &self.data[state.block_idx];
            let block_rows = block.num_rows(data_layout.row_width);

            if state.row_idx >= block_rows {
                state.block_idx += 1;
                state.row_idx = 0;
                continue;
            }

            let scan_count = usize::min(block_rows - state.row_idx, rem_cap);
            let selection = state.row_idx..(state.row_idx + scan_count);

            unsafe {
                state
                    .block_scan
                    .prepare_block_scan(block, data_layout.row_width, selection, false);
            }

            rem_cap -= scan_count;
            count += scan_count;
            state.row_idx += scan_count;
        }

        unsafe {
            data_layout.read_arrays(
                state.block_scan.row_pointers_iter(),
                output.arrays.iter_mut().enumerate(),
                0,
            )?
        }
        output.set_num_rows(count)?;

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::testutil::arrays::{assert_batches_eq, generate_batch, TestSortedRowBlock};

    #[test]
    fn scan_data_single_block_exact_capacity() {
        let batch = generate_batch!([2, 4, 1, 3], ["b", "d", "a", "c"]);
        let block = TestSortedRowBlock::from_batch(&batch, [0]);

        let run = SortedSegment::from_sorted_block(block.sorted_block);
        let mut state = run.init_scan_state();

        let mut out = Batch::new([DataType::Int32, DataType::Utf8], 4).unwrap();
        let count = run
            .scan_data(&mut state, &block.data_layout, &mut out)
            .unwrap();
        assert_eq!(4, count);

        let expected = generate_batch!([1, 2, 3, 4], ["a", "b", "c", "d"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn scan_data_single_block_small_capacity() {
        let batch = generate_batch!([2, 4, 1, 3], ["b", "d", "a", "c"]);
        let block = TestSortedRowBlock::from_batch(&batch, [0]);

        let run = SortedSegment::from_sorted_block(block.sorted_block);
        let mut state = run.init_scan_state();

        let mut out = Batch::new([DataType::Int32, DataType::Utf8], 3).unwrap();
        let count = run
            .scan_data(&mut state, &block.data_layout, &mut out)
            .unwrap();
        assert_eq!(3, count);

        let expected = generate_batch!([1, 2, 3], ["a", "b", "c"]);
        assert_batches_eq(&expected, &out);

        let count = run
            .scan_data(&mut state, &block.data_layout, &mut out)
            .unwrap();
        assert_eq!(1, count);

        let expected = generate_batch!([4], ["d"]);
        assert_batches_eq(&expected, &out);

        let count = run
            .scan_data(&mut state, &block.data_layout, &mut out)
            .unwrap();
        assert_eq!(0, count);
        assert_eq!(0, out.num_rows());
    }
}
