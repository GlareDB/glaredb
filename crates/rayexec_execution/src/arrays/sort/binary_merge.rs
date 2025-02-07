//! Iterative binary merge to produce a total order.

use rayexec_error::Result;

use super::sort_layout::SortLayout;
use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::row::block::Block;
use crate::arrays::row::row_layout::RowLayout;

/// Contains multiple blocks that have been totally sorted.
#[derive(Debug)]
pub struct SortedRun<B: BufferManager> {
    pub(crate) keys: Vec<Block<B>>,
    pub(crate) heap_keys: Vec<Block<B>>,
    pub(crate) heap_keys_heap: Vec<Block<B>>,
    pub(crate) data: Vec<Block<B>>,
    pub(crate) data_heap: Vec<Block<B>>,
}

/// Which side we should copy a row from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanSide {
    Left,
    Right,
}

#[derive(Debug)]
pub struct BinaryMergeState {
    left_scan: ScanState,
    right_scan: ScanState,
    /// Determines the side we should copy from when producing the output block.
    scan_sides: Vec<ScanSide>,
}

/// State for scanning one side of the merge.
#[derive(Debug, Clone)]
struct ScanState {
    /// Current block being scanned.
    block_idx: usize,
    /// Current row being scanned.
    row_idx: usize,
    /// Number of rows remaining in this sorted run.
    remaining: usize,
}

#[derive(Debug)]
pub struct BinaryMerger<'a, B: BufferManager> {
    pub(crate) manager: &'a B,
    pub(crate) key_layout: &'a SortLayout,
    pub(crate) data_layout: &'a RowLayout,
    /// Block capacity in rows for the resulting fixed-len blocks.
    ///
    /// Does not impact heap blocks.
    pub(crate) block_capacity: usize,
}

impl<'a, B> BinaryMerger<'a, B>
where
    B: BufferManager,
{
    /// Merge left and right, producing a new sorted run.
    pub fn merge(
        &self,
        state: &mut BinaryMergeState,
        mut left: SortedRun<B>,
        right: SortedRun<B>,
    ) -> Result<SortedRun<B>> {
        let scan_count = usize::min(
            self.block_capacity,
            state.left_scan.remaining + state.right_scan.remaining,
        );

        state.scan_sides.clear();
        state.scan_sides.resize(scan_count, ScanSide::Left);

        let mut merged_keys = Vec::new();
        let mut merged_heap_keys = Vec::new();
        let mut merged_data = Vec::new();

        loop {
            if state.left_scan.remaining == 0 && state.right_scan.remaining == 0 {
                break;
            }

            let (count, new_left_scan, new_right_scan) = self.find_merge_side(
                &left,
                &right,
                state.left_scan.clone(),
                state.right_scan.clone(),
                &mut state.scan_sides,
            )?;

            let scan_sides = &state.scan_sides[0..count];

            let key_block = self.merge_keys(
                &left,
                &right,
                state.left_scan.clone(),
                state.right_scan.clone(),
                scan_sides,
            )?;
            merged_keys.push(key_block);

            if self.key_layout.any_requires_heap() {
                let heap_key_block = self.merge_heap_keys(
                    &left,
                    &right,
                    state.left_scan.clone(),
                    state.right_scan.clone(),
                    scan_sides,
                )?;
                merged_heap_keys.push(heap_key_block);
            }

            let data_block = self.merge_data(
                &left,
                &right,
                state.left_scan.clone(),
                state.right_scan.clone(),
                scan_sides,
            )?;
            merged_data.push(data_block);

            state.left_scan = new_left_scan;
            state.right_scan = new_right_scan;
        }

        // Move heap blocks, we have active pointers from the row blocks, so
        // don't do anything to them.
        left.heap_keys_heap.extend(right.heap_keys_heap);
        left.data_heap.extend(right.data_heap);

        Ok(SortedRun {
            keys: merged_keys,
            heap_keys: merged_heap_keys,
            data: merged_data,
            heap_keys_heap: left.heap_keys_heap,
            data_heap: left.data_heap,
        })
    }

    /// Fills `source_sides` with which side to copy from for each row.
    ///
    /// Returns the number of elements filled.
    fn find_merge_side(
        &self,
        left: &SortedRun<B>,
        right: &SortedRun<B>,
        mut left_scan: ScanState,
        mut right_scan: ScanState,
        scan_sides: &mut [ScanSide],
    ) -> Result<(usize, ScanState, ScanState)> {
        let mut curr_count = 0;

        while curr_count < scan_sides.len() {
            // Move to next block if needed.
            let left_num_rows = left.keys[left_scan.block_idx].num_rows(self.key_layout.row_width);
            let right_num_rows =
                right.keys[right_scan.block_idx].num_rows(self.key_layout.row_width);

            if left_scan.row_idx == left_num_rows {
                left_scan.block_idx += 1;
                left_scan.row_idx = 0;
            }
            if right_scan.row_idx == right_num_rows {
                right_scan.block_idx += 1;
                right_scan.row_idx = 0;
            }

            let left_exhausted = left_scan.block_idx == left.keys.len();
            let right_exhausted = right_scan.block_idx == right.keys.len();

            if left_exhausted || right_exhausted {
                // We've reached either the end of the left or right runs, skip
                // comparing the rest.
                //
                // Still need to update left or right state to update the true
                // number of remaining rows.
                //
                // Note it shouldn't be possible for both sides to be exhausted
                // as `scan_sides` should have taking the total row count from
                // sides into account.
                debug_assert_ne!(left_exhausted, right_exhausted);
                let row_diff = scan_sides.len() - curr_count;
                if left_exhausted {
                    // Left exhausted, we're scanning `row_diff` additional
                    // rows from right.
                    right_scan.remaining -= row_diff;
                }
                if right_exhausted {
                    // Same but sides flipped.
                    left_scan.remaining -= row_diff;
                }

                break;
            }

            let left_block_ptr = left.keys[left_scan.block_idx].as_ptr();
            let mut left_ptr =
                unsafe { left_block_ptr.byte_add(self.key_layout.row_width * left_scan.row_idx) };
            let right_block_ptr = right.keys[right_scan.block_idx].as_ptr();
            let mut right_ptr =
                unsafe { right_block_ptr.byte_add(self.key_layout.row_width * right_scan.row_idx) };

            if !self.key_layout.any_requires_heap() {
                // Scan as much as we can until we exhaust either of the current
                // blocks we're scanning.
                while left_scan.row_idx < left_num_rows
                    && right_scan.row_idx < right_num_rows
                    && curr_count < scan_sides.len()
                {
                    let left_val =
                        unsafe { std::slice::from_raw_parts(left_ptr, self.key_layout.row_width) };
                    let right_val =
                        unsafe { std::slice::from_raw_parts(right_ptr, self.key_layout.row_width) };

                    if left_val < right_val {
                        scan_sides[curr_count] = ScanSide::Left;
                        left_scan.row_idx += 1;
                        left_scan.remaining -= 1;
                        unsafe { left_ptr = left_ptr.byte_add(self.key_layout.row_width) };
                    } else {
                        scan_sides[curr_count] = ScanSide::Right;
                        right_scan.row_idx += 1;
                        right_scan.remaining -= 1;
                        unsafe { right_ptr = right_ptr.byte_add(self.key_layout.row_width) };
                    }

                    curr_count += 1;
                }
            } else {
                // Need to check heap keys.
                unimplemented!()
            }
        }

        Ok((curr_count, left_scan, right_scan))
    }

    fn merge_keys(
        &self,
        left: &SortedRun<B>,
        right: &SortedRun<B>,
        left_scan: ScanState,
        right_scan: ScanState,
        scan_sides: &[ScanSide],
    ) -> Result<Block<B>> {
        let block_ptr_fn =
            |sorted_run: &SortedRun<B>, block_idx: usize| sorted_run.keys[block_idx].as_ptr();

        Self::merge_fixed_size_blocks(
            self.manager,
            self.key_layout.row_width,
            left,
            right,
            block_ptr_fn,
            left_scan,
            right_scan,
            scan_sides,
        )
    }

    fn merge_heap_keys(
        &self,
        left: &SortedRun<B>,
        right: &SortedRun<B>,
        left_scan: ScanState,
        right_scan: ScanState,
        scan_sides: &[ScanSide],
    ) -> Result<Block<B>> {
        let block_ptr_fn =
            |sorted_run: &SortedRun<B>, block_idx: usize| sorted_run.heap_keys[block_idx].as_ptr();

        Self::merge_fixed_size_blocks(
            self.manager,
            self.key_layout.row_width,
            left,
            right,
            block_ptr_fn,
            left_scan,
            right_scan,
            scan_sides,
        )
    }

    fn merge_data(
        &self,
        left: &SortedRun<B>,
        right: &SortedRun<B>,
        left_scan: ScanState,
        right_scan: ScanState,
        scan_sides: &[ScanSide],
    ) -> Result<Block<B>> {
        let block_ptr_fn =
            |sorted_run: &SortedRun<B>, block_idx: usize| sorted_run.data[block_idx].as_ptr();

        Self::merge_fixed_size_blocks(
            self.manager,
            self.key_layout.row_width,
            left,
            right,
            block_ptr_fn,
            left_scan,
            right_scan,
            scan_sides,
        )
    }

    /// Helper for merging fixed-sized blocks from the left and right runs.
    ///
    /// `block_ptr_fn` returns the pointer to a block at the given index.
    fn merge_fixed_size_blocks(
        manager: &B,
        row_width: usize,
        left: &SortedRun<B>,
        right: &SortedRun<B>,
        block_ptr_fn: impl Fn(&SortedRun<B>, usize) -> *const u8,
        mut left_scan: ScanState,
        mut right_scan: ScanState,
        scan_sides: &[ScanSide],
    ) -> Result<Block<B>> {
        // Output is exact size for holding the merge.
        let mut out = Block::try_new(manager, row_width * scan_sides.len())?;
        let mut curr_count = 0;

        while curr_count < scan_sides.len() {
            // Move to next block if needed.
            let left_num_rows = left.keys[left_scan.block_idx].num_rows(row_width);
            let right_num_rows = right.keys[right_scan.block_idx].num_rows(row_width);

            if left_scan.row_idx == left_num_rows {
                left_scan.block_idx += 1;
                left_scan.row_idx = 0;
            }
            if right_scan.row_idx == right_num_rows {
                right_scan.block_idx += 1;
                right_scan.row_idx = 0;
            }

            let out_block_ptr = out.as_mut_ptr();
            let mut out_ptr = unsafe { out_block_ptr.byte_add(row_width * curr_count) };

            let left_block_ptr = block_ptr_fn(left, left_scan.block_idx);
            let mut left_ptr = unsafe { left_block_ptr.byte_add(row_width * left_scan.row_idx) };
            let right_block_ptr = block_ptr_fn(right, right_scan.block_idx);
            let mut right_ptr = unsafe { right_block_ptr.byte_add(row_width * right_scan.row_idx) };

            // Scan as much as we can until we exhaust either of the current
            // blocks we're scanning.
            while left_scan.row_idx < left_num_rows
                && right_scan.row_idx < right_num_rows
                && curr_count < scan_sides.len()
            {
                match scan_sides[curr_count] {
                    ScanSide::Left => unsafe {
                        out_ptr.copy_from_nonoverlapping(left_ptr, row_width);
                        left_ptr = left_ptr.byte_add(row_width);
                    },
                    ScanSide::Right => unsafe {
                        out_ptr.copy_from_nonoverlapping(right_ptr, row_width);
                        right_ptr = right_ptr.byte_add(row_width);
                    },
                }

                unsafe {
                    out_ptr = out_ptr.byte_add(row_width);
                }
                curr_count += 1;
            }
        }

        Ok(out)
    }

    fn num_rows_in_run(&self, run: &SortedRun<B>) -> usize {
        run.keys
            .iter()
            .map(|b| b.num_rows(self.key_layout.row_width))
            .sum()
    }
}
