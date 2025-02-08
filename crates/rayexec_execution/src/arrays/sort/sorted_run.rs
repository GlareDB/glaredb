use rayexec_error::Result;

use super::sorted_block::SortedBlock;
use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::batch::Batch;
use crate::arrays::row::block::Block;
use crate::arrays::row::block_scanner::BlockScanState;
use crate::arrays::row::row_layout::RowLayout;

/// Scan state for scanning a sorted run.
#[derive(Debug)]
pub struct SortedRunScanState {
    block_idx: usize,
    row_idx: usize,
    block_scan: BlockScanState,
}

/// Contains multiple blocks that have been totally sorted.
#[derive(Debug)]
pub struct SortedRun<B: BufferManager> {
    pub(crate) keys: Vec<Block<B>>,
    pub(crate) heap_keys: Vec<Block<B>>,
    pub(crate) heap_keys_heap: Vec<Block<B>>,
    pub(crate) data: Vec<Block<B>>,
    pub(crate) data_heap: Vec<Block<B>>,
}

impl<B> SortedRun<B>
where
    B: BufferManager,
{
    /// Create a new sorted run from a sorted block.
    pub fn from_sorted_block(block: SortedBlock<B>) -> Self {
        SortedRun {
            keys: vec![block.keys],
            heap_keys: vec![block.heap_keys],
            heap_keys_heap: block.heap_keys_heap,
            data: vec![block.data],
            data_heap: block.data_heap,
        }
    }

    pub fn init_scan_state(&self) -> SortedRunScanState {
        SortedRunScanState {
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
        state: &mut SortedRunScanState,
        data_layout: &RowLayout,
        output: &mut Batch<B>,
    ) -> Result<usize> {
        let mut count = 0;
        let mut rem_cap = output.capacity;

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
            data_layout.read_arrays(&state.block_scan, output.arrays.iter_mut().enumerate(), 0)?
        }
        output.set_num_rows(count)?;

        Ok(count)
    }
}
