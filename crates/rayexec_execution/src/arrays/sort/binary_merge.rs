//! Iterative binary merge to produce a total order.

use rayexec_error::Result;

use super::sort_layout::SortLayout;
use super::sorted_block::SortedBlock;
use super::sorted_run::SortedRun;
use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::row::block::Block;
use crate::arrays::row::row_layout::RowLayout;

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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ScanState {
    /// Current block being scanned.
    block_idx: usize,
    /// Current row being scanned.
    row_idx: usize,
    /// Number of rows remaining in this sorted run.
    remaining: usize,
}

impl ScanState {
    fn reset_for_run<B>(&mut self, key_layout: &SortLayout, run: &SortedRun<B>)
    where
        B: BufferManager,
    {
        self.block_idx = 0;
        self.row_idx = 0;
        self.remaining = run
            .keys
            .iter()
            .map(|block| block.num_rows(key_layout.row_width))
            .sum();
    }
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
    pub fn new(
        manager: &'a B,
        key_layout: &'a SortLayout,
        data_layout: &'a RowLayout,
        block_capacity: usize,
    ) -> Self {
        BinaryMerger {
            manager,
            key_layout,
            data_layout,
            block_capacity,
        }
    }

    pub fn init_merge_state(&self) -> BinaryMergeState {
        BinaryMergeState {
            left_scan: ScanState::default(),
            right_scan: ScanState::default(),
            scan_sides: Vec::new(),
        }
    }

    /// Merge left and right, producing a new sorted run.
    ///
    /// This will internally reset the provided state.
    pub fn merge(
        &self,
        state: &mut BinaryMergeState,
        mut left: SortedRun<B>,
        right: SortedRun<B>,
    ) -> Result<SortedRun<B>> {
        state.left_scan.reset_for_run(self.key_layout, &left);
        state.right_scan.reset_for_run(self.key_layout, &right);

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

            let (interleave_count, find_left, find_right) = self.find_merge_side(
                &left,
                &right,
                state.left_scan.clone(),
                state.right_scan.clone(),
                &mut state.scan_sides,
            )?;

            let scan_sides = &state.scan_sides[0..interleave_count];

            let (key_block, _, _) = self.merge_keys(
                &left,
                &right,
                state.left_scan.clone(),
                state.right_scan.clone(),
                scan_count,
                scan_sides,
            )?;
            merged_keys.push(key_block);

            if self.key_layout.any_requires_heap() {
                let (heap_key_block, _, _) = self.merge_heap_keys(
                    &left,
                    &right,
                    state.left_scan.clone(),
                    state.right_scan.clone(),
                    scan_count,
                    scan_sides,
                )?;
                merged_heap_keys.push(heap_key_block);
            }

            let (data_block, new_left_scan, new_right_scan) = self.merge_data(
                &left,
                &right,
                state.left_scan.clone(),
                state.right_scan.clone(),
                scan_count,
                scan_sides,
            )?;
            merged_data.push(data_block);

            // All updated left/right scans returned from the above `merge`
            // should be the same as the left/right scan from when we computed
            // the scan sides.
            //
            // The updates are duplicated across all merge functions, but it
            // seemed easier to pass in a cloned state and have each update
            // independently than trying to reset a single state.
            assert_eq!(find_left, new_left_scan);
            assert_eq!(find_right, new_right_scan);

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
    /// Returns the number of elements filled in `scan_sides`. If this is less
    /// than the len of `scan_sides`, then that indicates either the left or
    /// right were exhausted.
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
        scan_count: usize,
        scan_sides: &[ScanSide],
    ) -> Result<(Block<B>, ScanState, ScanState)> {
        fn block_fn<B: BufferManager>(sorted_run: &SortedRun<B>, block_idx: usize) -> &Block<B> {
            &sorted_run.keys[block_idx]
        }

        Self::merge_fixed_size_blocks(
            self.manager,
            self.key_layout.row_width,
            left,
            right,
            block_fn,
            left_scan,
            right_scan,
            scan_count,
            scan_sides,
        )
    }

    fn merge_heap_keys(
        &self,
        left: &SortedRun<B>,
        right: &SortedRun<B>,
        left_scan: ScanState,
        right_scan: ScanState,
        scan_count: usize,
        scan_sides: &[ScanSide],
    ) -> Result<(Block<B>, ScanState, ScanState)> {
        fn block_fn<B: BufferManager>(sorted_run: &SortedRun<B>, block_idx: usize) -> &Block<B> {
            &sorted_run.heap_keys[block_idx]
        }

        Self::merge_fixed_size_blocks(
            self.manager,
            self.key_layout.row_width,
            left,
            right,
            block_fn,
            left_scan,
            right_scan,
            scan_count,
            scan_sides,
        )
    }

    fn merge_data(
        &self,
        left: &SortedRun<B>,
        right: &SortedRun<B>,
        left_scan: ScanState,
        right_scan: ScanState,
        scan_count: usize,
        scan_sides: &[ScanSide],
    ) -> Result<(Block<B>, ScanState, ScanState)> {
        fn block_fn<B: BufferManager>(sorted_run: &SortedRun<B>, block_idx: usize) -> &Block<B> {
            &sorted_run.data[block_idx]
        }

        Self::merge_fixed_size_blocks(
            self.manager,
            self.key_layout.row_width,
            left,
            right,
            block_fn,
            left_scan,
            right_scan,
            scan_count,
            scan_sides,
        )
    }

    /// Helper for merging fixed-sized blocks from the left and right runs.
    ///
    /// `block_fn` returns the pointer to a block at the given index.
    ///
    /// Returns the output block, as well as an updated left/right scan state.
    fn merge_fixed_size_blocks(
        manager: &B,
        row_width: usize,
        left: &SortedRun<B>,
        right: &SortedRun<B>,
        block_fn: impl Fn(&SortedRun<B>, usize) -> &Block<B>,
        mut left_scan: ScanState,
        mut right_scan: ScanState,
        scan_count: usize,
        scan_sides: &[ScanSide],
    ) -> Result<(Block<B>, ScanState, ScanState)> {
        debug_assert!(left_scan.remaining + right_scan.remaining >= scan_sides.len());

        // Output is exact size for holding the merge.
        let mut out = Block::try_new(manager, row_width * scan_sides.len())?;
        let mut curr_count = 0;

        while curr_count < scan_sides.len() {
            let left_block = block_fn(left, left_scan.block_idx);
            let right_block = block_fn(right, right_scan.block_idx);

            // Move to next block if needed.
            let left_num_rows = left_block.num_rows(row_width);
            let right_num_rows = right_block.num_rows(row_width);

            if left_scan.row_idx == left_num_rows {
                left_scan.block_idx += 1;
                left_scan.row_idx = 0;
                continue;
            }
            if right_scan.row_idx == right_num_rows {
                right_scan.block_idx += 1;
                right_scan.row_idx = 0;
                continue;
            }

            let out_block_ptr = out.as_mut_ptr();
            let mut out_ptr = unsafe { out_block_ptr.byte_add(row_width * curr_count) };

            let left_block_ptr = left_block.as_ptr();
            let mut left_ptr = unsafe { left_block_ptr.byte_add(row_width * left_scan.row_idx) };
            let right_block_ptr = right_block.as_ptr();
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

                        left_scan.row_idx += 1;
                        left_scan.remaining -= 1;
                    },
                    ScanSide::Right => unsafe {
                        out_ptr.copy_from_nonoverlapping(right_ptr, row_width);
                        right_ptr = right_ptr.byte_add(row_width);

                        right_scan.row_idx += 1;
                        right_scan.remaining -= 1;
                    },
                }

                unsafe {
                    out_ptr = out_ptr.byte_add(row_width);
                }
                curr_count += 1;
            }
        }

        if scan_count > scan_sides.len() {
            // One side is exhausted, need to copy in bulk from the non-exausted
            // side.
            let left_exhausted = left_scan.block_idx == left.keys.len();
            let right_exhausted = right_scan.block_idx == right.keys.len();
            debug_assert_ne!(left_exhausted, right_exhausted); // Only one side should be exhausted.

            let rem_rows = scan_count - scan_sides.len();

            if left_exhausted {
                // Bulk copy rows from the right.
                right_scan = Self::bulk_copy(
                    row_width, right, right_scan, block_fn, &mut out, curr_count, rem_rows,
                )?;
            } else {
                // Bulk copy rows from the left.
                left_scan = Self::bulk_copy(
                    row_width, left, left_scan, block_fn, &mut out, curr_count, rem_rows,
                )?;
            }
        }

        Ok((out, left_scan, right_scan))
    }

    fn bulk_copy(
        row_width: usize,
        src: &SortedRun<B>,
        mut src_scan: ScanState,
        block_fn: impl Fn(&SortedRun<B>, usize) -> &Block<B>,
        out: &mut Block<B>,
        curr_count: usize,
        mut rem_rows: usize,
    ) -> Result<ScanState> {
        src_scan.remaining -= rem_rows;

        let out_block_ptr = out.as_mut_ptr();
        let mut out_ptr = unsafe { out_block_ptr.byte_add(row_width * curr_count) };

        while rem_rows > 0 {
            let block = block_fn(src, src_scan.block_idx);

            // Move to next block if needed.
            let num_rows = block.num_rows(row_width);
            if src_scan.row_idx == num_rows {
                src_scan.block_idx += 1;
                src_scan.row_idx = 0;
                continue;
            }

            let src_ptr = unsafe { block.as_ptr().byte_add(row_width * src_scan.block_idx) };

            let copy_count = usize::min(num_rows, rem_rows);
            unsafe {
                out_ptr.copy_from_nonoverlapping(src_ptr, row_width * copy_count);
                out_ptr = out_ptr.byte_add(row_width * copy_count);
            }

            rem_rows -= copy_count;
            src_scan.row_idx += copy_count;
        }

        Ok(src_scan)
    }
}
