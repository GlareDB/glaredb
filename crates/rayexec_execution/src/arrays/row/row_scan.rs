use std::borrow::BorrowMut;
use std::collections::VecDeque;

use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use super::block::ValidityInitializer;
use super::block_scan::BlockScanState;
use super::row_blocks::RowBlocks;
use super::row_layout::RowLayout;
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::Array;

/// State for resumable scanning of a set of row blocks conforming to some row
/// layout.
///
/// This enables shared logic when scanning a `RowCollection` and
/// `AggregateColection`.
///
/// When used with `AggregateCollection`, this scan state can be used to scan
/// group values since group values follow a `RowLayout` and are placed at the
/// beginning of rows.
#[derive(Debug)]
pub struct RowScanState {
    /// Remaining block indices to scan.
    pub(crate) blocks_to_scan: VecDeque<usize>,
    /// Current block we're scanning.
    ///
    /// If None, we should get the next block to scan.
    pub(crate) current_block: Option<usize>,
    /// Row index of the most recent row scanned within the block.
    pub(crate) row_idx: usize,
    /// State containing the pointers for the most recent scan.
    pub(crate) block_read: BlockScanState,
}

impl RowScanState {
    pub const fn new() -> Self {
        RowScanState {
            blocks_to_scan: VecDeque::new(),
            current_block: None,
            row_idx: 0,
            block_read: BlockScanState {
                row_pointers: Vec::new(),
            },
        }
    }

    /// Returns the row pointers for the most recent scan.
    pub fn scanned_row_pointers(&self) -> &[*const u8] {
        &self.block_read.row_pointers
    }

    pub fn scanned_row_pointers_mut(&mut self) -> &mut [*mut u8] {
        let s = self.block_read.row_pointers.as_mut_slice();
        unsafe { std::slice::from_raw_parts_mut(s.as_mut_ptr() as *mut *mut u8, s.len()) }
    }

    /// Resets the state for a full scan of the given set of row blocks.
    pub(crate) fn reset_for_full_scan(
        &mut self,
        row_blocks: &RowBlocks<NopBufferManager, ValidityInitializer>,
    ) {
        self.reset_for_partial_scan(0..row_blocks.num_row_blocks());
    }

    /// Resets this state to allow for a partial scan of a set of blocks.
    pub(crate) fn reset_for_partial_scan(
        &mut self,
        block_indices: impl IntoIterator<Item = usize>,
    ) {
        self.current_block = None;
        self.row_idx = 0;
        self.block_read.clear();

        self.blocks_to_scan.clear();
        self.blocks_to_scan.extend(block_indices);
    }

    /// Scan columns from the set of row blocks.
    ///
    /// This is only valid to call with the set of rows blocks this state was
    /// initialized with.
    ///
    /// Returns the number of rows scanned. A return value of zero indicates
    /// we've completed the scan.
    ///
    /// `count` indicates the max number of rows to write to the array. This
    /// must be less than or equal to the array capacities.
    pub fn scan<A>(
        &mut self,
        layout: &RowLayout,
        row_blocks: &RowBlocks<NopBufferManager, ValidityInitializer>,
        outputs: &mut [A],
        count: usize,
    ) -> Result<usize>
    where
        A: BorrowMut<Array>,
    {
        debug_assert_eq!(layout.num_columns(), outputs.len());
        let cols = outputs.len();
        self.scan_subset(layout, row_blocks, 0..cols, outputs, count)
    }

    /// Like `scan`, but for only scanning a subset of columns.
    ///
    /// `columns` indicates which columns to scan.
    ///
    /// Length of `outputs` must match length of `columns`.
    pub fn scan_subset<'a, A>(
        &mut self,
        layout: &RowLayout,
        row_blocks: &RowBlocks<NopBufferManager, ValidityInitializer>,
        columns: impl IntoExactSizeIterator<Item = usize> + Clone,
        outputs: &mut [A],
        count: usize,
    ) -> Result<usize>
    where
        A: BorrowMut<Array> + 'a,
    {
        debug_assert_eq!(columns.clone().into_exact_size_iter().len(), outputs.len());

        let mut scanned_count = 0;
        let mut remaining_cap = count;

        while remaining_cap > 0 {
            // Get the current block to scan.
            //
            // If None, we try to get the next block from the queue and reset
            // the row idx to 0 so that we start scanning that block from the
            // state.
            let current_block = match self.current_block {
                Some(curr) => curr,
                None => {
                    self.row_idx = 0;
                    let new_block = match self.blocks_to_scan.pop_front() {
                        Some(block) => block,
                        None => break,
                    };
                    self.current_block = Some(new_block);
                    self.current_block.unwrap()
                }
            };

            let num_rows = row_blocks.rows_in_row_block(current_block);
            if self.row_idx >= num_rows {
                // No more rows to scan in this chunk, move to next chunk.
                self.current_block = None;
                continue;
            }

            let scan_count = usize::min(remaining_cap, num_rows - self.row_idx);
            row_blocks.prepare_read(
                &mut self.block_read,
                current_block,
                self.row_idx..(self.row_idx + scan_count),
            )?;

            let columns = columns.clone();
            let outputs = columns.into_iter().zip(outputs.iter_mut());

            unsafe {
                layout.read_arrays(self.block_read.row_pointers_iter(), outputs, scanned_count)?;
            }

            // Update state.
            self.row_idx += scan_count;
            remaining_cap -= scan_count;
            scanned_count += scan_count;
        }

        Ok(scanned_count)
    }
}
