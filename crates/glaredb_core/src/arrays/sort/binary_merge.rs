use std::cmp::Ordering;

use glaredb_error::{OptionExt, Result};

use super::heap_compare::compare_heap_values;
use super::sort_layout::SortLayout;
use super::sorted_segment::SortedSegment;
use crate::arrays::bitmap::view::BitmapView;
use crate::arrays::row::block::Block;
use crate::arrays::row::row_layout::RowLayout;
use crate::buffer::buffer_manager::{AsRawBufferManager, RawBufferManager};

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
    /// Resets the state for a new sorted segment.
    fn reset_for_run(&mut self, key_layout: &SortLayout, run: &SortedSegment) {
        self.block_idx = 0;
        self.row_idx = 0;
        self.remaining = run
            .keys
            .iter()
            .map(|block| block.num_rows(key_layout.row_width))
            .sum();
    }
}

/// Merger for merging two sorted runs into a single sorted run.
#[derive(Debug)]
pub struct BinaryMerger<'a> {
    pub(crate) manager: RawBufferManager,
    pub(crate) key_layout: &'a SortLayout,
    pub(crate) data_layout: &'a RowLayout,
    /// Block capacity in rows for the resulting fixed-len blocks.
    ///
    /// Does not impact heap blocks.
    pub(crate) block_capacity: usize,
}

impl<'a> BinaryMerger<'a> {
    pub fn new(
        manager: &impl AsRawBufferManager,
        key_layout: &'a SortLayout,
        data_layout: &'a RowLayout,
        block_capacity: usize,
    ) -> Self {
        BinaryMerger {
            manager: manager.as_raw_buffer_manager(),
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
    ///
    /// If `limit_hint` is provided, then the merge may stop early to abide by
    /// the limit. The output segment will still represent the top rows
    /// according to the sort. The segment may contain more than the number of
    /// rows requested.
    pub fn merge(
        &self,
        state: &mut BinaryMergeState,
        mut left: SortedSegment,
        right: SortedSegment,
        limit_hint: Option<usize>,
    ) -> Result<SortedSegment> {
        state.left_scan.reset_for_run(self.key_layout, &left);
        state.right_scan.reset_for_run(self.key_layout, &right);

        let mut merged_keys = Vec::new();
        let mut merged_heap_keys = Vec::new();
        let mut merged_data = Vec::new();

        let mut total_scan_count = 0;

        loop {
            // Scan count used to build blocks for this iteration of the loop.
            let scan_count = usize::min(
                self.block_capacity,
                state.left_scan.remaining + state.right_scan.remaining,
            );
            state.scan_sides.clear();
            state.scan_sides.resize(scan_count, ScanSide::Left);

            if state.left_scan.remaining == 0 && state.right_scan.remaining == 0 {
                break;
            }

            if let Some(limit_hint) = limit_hint
                && total_scan_count >= limit_hint {
                    // We've scanned at least the requested number of rows.
                    //
                    // Return whatever we have in the segment now. All other
                    // remaining blocks will be dropped.
                    break;
                }

            // Fill which sides to scan from for each row.
            //
            // Returns updated states, however we need to use the original
            // states for rest of the merge operations in this iteration.
            //
            // TODO: We should align the states returned from this with the ones
            // returned from the real merges so that we can assert equality.
            let (_unused_l1, _unused_r1) = self.find_merge_side(
                &left,
                &right,
                state.left_scan.clone(),
                state.right_scan.clone(),
                &mut state.scan_sides,
            )?;

            // Merge the key blocks.
            //
            // Same as above, returns new states, but wee need to keep using the
            // old ones.
            let (key_block, _unused_l2, _unused_r2) = self.merge_keys(
                &left,
                &right,
                state.left_scan.clone(),
                state.right_scan.clone(),
                &state.scan_sides,
            )?;
            // TODO: Uncomment when they're actually equal
            // debug_assert_eq!(_unused_l1, _unused_l2);
            // debug_assert_eq!(_unused_r1, _unused_r2);
            merged_keys.push(key_block);

            if self.key_layout.any_requires_heap() {
                // Merge the heap keys.
                let (heap_key_block, _unused_l3, _unused_r3) = self.merge_heap_keys(
                    &left,
                    &right,
                    state.left_scan.clone(),
                    state.right_scan.clone(),
                    &state.scan_sides,
                )?;
                assert_eq!(_unused_l2, _unused_l3);
                assert_eq!(_unused_r2, _unused_r3);
                merged_heap_keys.push(heap_key_block);
            }

            // Finally merge the data. The updated scan states are what we'll
            // used in the next iteration of this loop.
            let (data_block, new_left_scan, new_right_scan) = self.merge_data(
                &left,
                &right,
                state.left_scan.clone(),
                state.right_scan.clone(),
                &state.scan_sides,
            )?;
            assert_eq!(_unused_l2, new_left_scan);
            assert_eq!(_unused_r2, new_right_scan);
            merged_data.push(data_block);

            // All updated left/right scans returned from the above `merge`
            // should be the same as the left/right scan from when we computed
            // the scan sides.
            //
            // The updates are duplicated across all merge functions, but it
            // seemed easier to pass in a cloned state and have each update
            // independently than trying to reset a single state.

            state.left_scan = new_left_scan;
            state.right_scan = new_right_scan;

            // For the possible limit.
            total_scan_count += scan_count;
        }

        // Move heap blocks, we have active pointers from the row blocks, so
        // don't do anything to them.
        left.heap_keys_heap.extend(right.heap_keys_heap);
        left.data_heap.extend(right.data_heap);

        Ok(SortedSegment {
            keys: merged_keys,
            heap_keys: merged_heap_keys,
            data: merged_data,
            heap_keys_heap: left.heap_keys_heap,
            data_heap: left.data_heap,
        })
    }

    /// Fills `source_sides` with which side to copy from for each row.
    fn find_merge_side(
        &self,
        left: &SortedSegment,
        right: &SortedSegment,
        mut left_scan: ScanState,
        mut right_scan: ScanState,
        scan_sides: &mut [ScanSide],
    ) -> Result<(ScanState, ScanState)> {
        let mut curr_count = 0;

        while curr_count < scan_sides.len() {
            // Move to next block if needed.
            let left_block = match left.keys.get(left_scan.block_idx) {
                Some(block) => block,
                None => {
                    // Left segment exhausted.
                    debug_assert_eq!(0, left_scan.remaining);
                    break;
                }
            };
            let right_block = match right.keys.get(right_scan.block_idx) {
                Some(block) => block,
                None => {
                    // Right segment exhausted.
                    debug_assert_eq!(0, right_scan.remaining);
                    break;
                }
            };

            // Move to next block if needed.
            let left_num_rows = left_block.num_rows(self.key_layout.row_width);
            let right_num_rows = right_block.num_rows(self.key_layout.row_width);

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
                // Similarly to above, scan as much as we can.
                while left_scan.row_idx < left_num_rows
                    && right_scan.row_idx < right_num_rows
                    && curr_count < scan_sides.len()
                {
                    // Sort layout contains heap keys. Need to compare column by
                    // column since we may need look at the row-encoded heap value
                    // to break ties.
                    let mut col_left_ptr = left_ptr;
                    let mut col_right_ptr = right_ptr;

                    let mut col_cmp = Ordering::Equal;

                    for col_idx in 0..self.key_layout.columns.len() {
                        // Do the initial comparison of the columns.
                        let col_width = self.key_layout.column_widths[col_idx];
                        let col_left =
                            unsafe { std::slice::from_raw_parts(col_left_ptr, col_width) };
                        let col_right =
                            unsafe { std::slice::from_raw_parts(col_right_ptr, col_width) };

                        col_cmp = col_left.cmp(col_right);
                        if col_cmp == Ordering::Equal
                            && self.key_layout.column_requires_heap(col_idx)
                        {
                            // Need to check the heap value.
                            col_cmp = Self::compare_heap_key(
                                self.key_layout,
                                col_idx,
                                &left_scan,
                                &right_scan,
                                left,
                                right,
                            )?
                        }

                        if col_cmp == Ordering::Less || col_cmp == Ordering::Greater {
                            // We have our ordering, no need to check the rest
                            // of the columns.
                            break;
                        }

                        // Advance pointers to next column to check.
                        col_left_ptr = unsafe { col_left_ptr.byte_add(col_width) };
                        col_right_ptr = unsafe { col_right_ptr.byte_add(col_width) };
                    }

                    // Same logic as without the heap keys.
                    match col_cmp {
                        Ordering::Less => {
                            scan_sides[curr_count] = ScanSide::Left;
                            left_scan.row_idx += 1;
                            left_scan.remaining -= 1;
                            unsafe { left_ptr = left_ptr.byte_add(self.key_layout.row_width) };
                        }
                        _ => {
                            scan_sides[curr_count] = ScanSide::Right;
                            right_scan.row_idx += 1;
                            right_scan.remaining -= 1;
                            unsafe { right_ptr = right_ptr.byte_add(self.key_layout.row_width) };
                        }
                    }
                    curr_count += 1;
                }
            }
        }

        // If curr count is less than scan sides, we know we've reached the end
        // of of one segment.
        if curr_count < scan_sides.len() {
            let left_exhausted = left_scan.remaining == 0;
            let right_exhausted = right_scan.remaining == 0;

            assert!(left_exhausted || right_exhausted); // One side should have been exhuasted...
            assert_ne!(left_exhausted, right_exhausted); // And only one side.

            // We've reached either the end of the left or right runs, skip
            // comparing the rest.
            //
            // Still need to update left or right state to update the true
            // number of remaining rows.
            //
            // Note it shouldn't be possible for both sides to be exhausted
            // as `scan_sides` should have taking the total row count from
            // sides into account.
            let row_diff = scan_sides.len() - curr_count;
            if left_exhausted {
                // Left exhausted, we're scanning `row_diff` additional
                // rows from right.
                right_scan.remaining -= row_diff;
                scan_sides[curr_count..].fill(ScanSide::Right);
            }
            if right_exhausted {
                // Same but sides flipped.
                left_scan.remaining -= row_diff;
                scan_sides[curr_count..].fill(ScanSide::Left);
            }
        }

        Ok((left_scan, right_scan))
    }

    fn merge_keys(
        &self,
        left: &SortedSegment,
        right: &SortedSegment,
        left_scan: ScanState,
        right_scan: ScanState,
        scan_sides: &[ScanSide],
    ) -> Result<(Block, ScanState, ScanState)> {
        fn block_fn(sorted_run: &SortedSegment, block_idx: usize) -> Option<&Block> {
            sorted_run.keys.get(block_idx)
        }

        Self::merge_fixed_size_blocks(
            &self.manager,
            self.key_layout.row_width,
            left,
            right,
            block_fn,
            left_scan,
            right_scan,
            scan_sides,
        )
    }

    fn merge_heap_keys(
        &self,
        left: &SortedSegment,
        right: &SortedSegment,
        left_scan: ScanState,
        right_scan: ScanState,
        scan_sides: &[ScanSide],
    ) -> Result<(Block, ScanState, ScanState)> {
        fn block_fn(sorted_run: &SortedSegment, block_idx: usize) -> Option<&Block> {
            sorted_run.heap_keys.get(block_idx)
        }

        Self::merge_fixed_size_blocks(
            &self.manager,
            self.key_layout.heap_layout.row_width,
            left,
            right,
            block_fn,
            left_scan,
            right_scan,
            scan_sides,
        )
    }

    fn merge_data(
        &self,
        left: &SortedSegment,
        right: &SortedSegment,
        left_scan: ScanState,
        right_scan: ScanState,
        scan_sides: &[ScanSide],
    ) -> Result<(Block, ScanState, ScanState)> {
        fn block_fn(sorted_run: &SortedSegment, block_idx: usize) -> Option<&Block> {
            sorted_run.data.get(block_idx)
        }

        Self::merge_fixed_size_blocks(
            &self.manager,
            self.data_layout.row_width,
            left,
            right,
            block_fn,
            left_scan,
            right_scan,
            scan_sides,
        )
    }

    /// Helper for merging fixed-sized blocks from the left and right runs.
    ///
    /// `block_fn` returns the pointer to a block at the given index.
    ///
    /// Returns the output block, as well as an updated left/right scan state.
    #[allow(clippy::too_many_arguments)] // I know
    fn merge_fixed_size_blocks(
        manager: &RawBufferManager,
        row_width: usize,
        left: &SortedSegment,
        right: &SortedSegment,
        block_fn: impl Fn(&SortedSegment, usize) -> Option<&Block>,
        mut left_scan: ScanState,
        mut right_scan: ScanState,
        scan_sides: &[ScanSide],
    ) -> Result<(Block, ScanState, ScanState)> {
        assert!(left_scan.remaining + right_scan.remaining >= scan_sides.len());

        // Output is exact size for holding the merge.
        let mut out = Block::try_new_reserve_all(manager, row_width * scan_sides.len())?;
        let mut curr_count = 0;

        while curr_count < scan_sides.len() {
            let left_block = match block_fn(left, left_scan.block_idx) {
                Some(block) => block,
                None => {
                    // Left segment exhausted.
                    debug_assert_eq!(0, left_scan.remaining);
                    break;
                }
            };
            let right_block = match block_fn(right, right_scan.block_idx) {
                Some(block) => block,
                None => {
                    // Right segment exhausted.
                    debug_assert_eq!(0, right_scan.remaining);
                    break;
                }
            };

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
                unsafe { out_ptr = out_ptr.byte_add(row_width) };
                curr_count += 1;
            }
            // Move to next iteration of the loop, we'll move to the next block
            // as needed.
        }

        if curr_count < scan_sides.len() {
            // One side is exhausted, need to copy in bulk from the non-exausted
            // side.
            let left_exhausted = left_scan.remaining == 0;
            let right_exhausted = right_scan.remaining == 0;
            assert!(left_exhausted || right_exhausted); // One side should have been exhuasted...
            assert_ne!(left_exhausted, right_exhausted); // And only one side.

            // If one side is exhausted, then the remainder of scan_sides should
            // indicate scanning from the other side.
            debug_assert!(if left_exhausted {
                scan_sides[curr_count..]
                    .iter()
                    .all(|&s| s == ScanSide::Right)
            } else {
                scan_sides[curr_count..]
                    .iter()
                    .all(|&s| s == ScanSide::Left)
            });

            let rem_rows = scan_sides.len() - curr_count;

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
        src: &SortedSegment,
        mut src_scan: ScanState,
        block_fn: impl Fn(&SortedSegment, usize) -> Option<&Block>,
        out: &mut Block,
        curr_count: usize,
        mut rem_rows: usize,
    ) -> Result<ScanState> {
        src_scan.remaining -= rem_rows;

        let out_block_ptr = out.as_mut_ptr();
        let mut out_ptr = unsafe { out_block_ptr.byte_add(row_width * curr_count) };

        while rem_rows > 0 {
            let block = block_fn(src, src_scan.block_idx)
                .required("Block must exist for bulk copying remaining rows")?;

            // Move to next block if needed.
            let num_rows = block.num_rows(row_width);
            if src_scan.row_idx == num_rows {
                src_scan.block_idx += 1;
                src_scan.row_idx = 0;
                continue;
            }

            let src_ptr = unsafe { block.as_ptr().byte_add(row_width * src_scan.row_idx) };

            let rem_in_block = num_rows - src_scan.row_idx;
            let copy_count = usize::min(rem_in_block, rem_rows);
            unsafe {
                out_ptr.copy_from_nonoverlapping(src_ptr, row_width * copy_count);
                out_ptr = out_ptr.byte_add(row_width * copy_count);
            }

            rem_rows -= copy_count;
            src_scan.row_idx += copy_count;
        }

        Ok(src_scan)
    }

    /// Compare a heap key between left and right.
    ///
    /// `key_column` is the the index of the key in the sort layout, not the row
    /// layout.
    fn compare_heap_key(
        layout: &SortLayout,
        key_column: usize,
        left_scan_state: &ScanState,
        right_scan_state: &ScanState,
        left: &SortedSegment,
        right: &SortedSegment,
    ) -> Result<Ordering> {
        let heap_key_column = layout.heap_mapping[key_column]
            .expect("to be provided a column index that has a heap key");

        let left_heap_block = &left.heap_keys[left_scan_state.block_idx];
        let right_heap_block = &right.heap_keys[right_scan_state.block_idx];

        let left_row_ptr = unsafe {
            left_heap_block
                .as_ptr()
                .byte_add(layout.heap_layout.row_width * left_scan_state.row_idx)
        };
        let right_row_ptr = unsafe {
            right_heap_block
                .as_ptr()
                .byte_add(layout.heap_layout.row_width * right_scan_state.row_idx)
        };

        let left_validity_buf = unsafe { layout.heap_layout.validity_buffer(left_row_ptr) };
        let right_validity_buf = unsafe { layout.heap_layout.validity_buffer(right_row_ptr) };

        let left_valid = BitmapView::new(left_validity_buf, layout.heap_layout.num_columns())
            .value(heap_key_column);
        let right_valid = BitmapView::new(right_validity_buf, layout.heap_layout.num_columns())
            .value(heap_key_column);

        // It's only possible for the left and right values to both be NULL, or
        // neither be NULL.
        //
        // If one value is NULL, then we should have been able to short-circuit
        // with the prefix comparison.
        if !left_valid && !right_valid {
            return Ok(Ordering::Equal);
        }

        let col_offset = layout.heap_layout.offsets[heap_key_column];
        let left_col_ptr = unsafe { left_row_ptr.byte_add(col_offset) };
        let right_col_ptr = unsafe { right_row_ptr.byte_add(col_offset) };

        let phys_type = layout.heap_layout.types[heap_key_column].physical_type()?;
        let ord = unsafe { compare_heap_values(left_col_ptr, right_col_ptr, phys_type)? };

        // Note we're using the original key column index.
        if layout.columns[key_column].desc {
            Ok(ord.reverse())
        } else {
            Ok(ord)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::testutil::arrays::{TestSortedRowBlock, assert_batches_eq, generate_batch};

    /// Helper that will binary merge left and right, returning the result.
    ///
    /// Note that this will place the left and right batches into their own
    /// sorted blocks, which will locally sort the batch prior to the binary
    /// merge.
    ///
    /// This will initialize everything to the exact capacity needed in the
    /// output block, meaning we'll only hit the merge loop once.
    fn binary_merge_exact_cap(
        left: &Batch,
        left_keys: impl IntoIterator<Item = usize>,
        right: &Batch,
        right_keys: impl IntoIterator<Item = usize>,
    ) -> Batch {
        let cap = left.num_rows() + right.num_rows();

        let left_block = TestSortedRowBlock::from_batch(left, left_keys);
        let right_block = TestSortedRowBlock::from_batch(right, right_keys);

        let left_run = SortedSegment::from_sorted_block(left_block.sorted_block);
        let right_run = SortedSegment::from_sorted_block(right_block.sorted_block);

        let merger = BinaryMerger::new(
            &DefaultBufferManager,
            &left_block.key_layout,
            &left_block.data_layout,
            cap,
        );
        let mut state = merger.init_merge_state();
        let out = merger.merge(&mut state, left_run, right_run, None).unwrap();
        assert_eq!(1, out.keys.len());

        let mut scan = out.init_scan_state();
        let mut out_batch = Batch::new([DataType::int32(), DataType::utf8()], cap).unwrap();
        out.scan_data(&mut scan, &left_block.data_layout, &mut out_batch)
            .unwrap();

        out_batch
    }

    #[test]
    fn binary_merge_odd() {
        let left = generate_batch!([2], ["b"]);
        let right = generate_batch!([1, 3], ["a", "c"]);
        let out = binary_merge_exact_cap(&left, [0], &right, [0]);

        let expected = generate_batch!([1, 2, 3], ["a", "b", "c"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn binary_merge_odd_flipped() {
        let left = generate_batch!([1, 3], ["a", "c"]);
        let right = generate_batch!([2], ["b"]);
        let out = binary_merge_exact_cap(&left, [0], &right, [0]);

        let expected = generate_batch!([1, 2, 3], ["a", "b", "c"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn binary_merge_interleave() {
        let left = generate_batch!([1, 3, 5], ["a", "c", "e"]);
        let right = generate_batch!([2, 4, 6], ["b", "d", "f"]);
        let out = binary_merge_exact_cap(&left, [0], &right, [0]);

        let expected = generate_batch!([1, 2, 3, 4, 5, 6], ["a", "b", "c", "d", "e", "f"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn binary_merge_interleave_flipped() {
        // Same as above, but left/right has data flipped. This ensure we
        // properly copy the last element from the left (6, "f").

        let left = generate_batch!([2, 4, 6], ["b", "d", "f"]);
        let right = generate_batch!([1, 3, 5], ["a", "c", "e"]);
        let out = binary_merge_exact_cap(&left, [0], &right, [0]);

        let expected = generate_batch!([1, 2, 3, 4, 5, 6], ["a", "b", "c", "d", "e", "f"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn binary_merge_interleave_2() {
        let left = generate_batch!([1, 4, 5], ["a", "d", "e"]);
        let right = generate_batch!([2, 3, 6], ["b", "c", "f"]);
        let out = binary_merge_exact_cap(&left, [0], &right, [0]);

        let expected = generate_batch!([1, 2, 3, 4, 5, 6], ["a", "b", "c", "d", "e", "f"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn binary_merge_presorted() {
        // Left and right sort one after another, tests the we properly bulk
        // copy more than one row.

        let left = generate_batch!([1, 2, 3], ["a", "b", "c"]);
        let right = generate_batch!([4, 5, 6], ["d", "e", "f"]);
        let out = binary_merge_exact_cap(&left, [0], &right, [0]);

        let expected = generate_batch!([1, 2, 3, 4, 5, 6], ["a", "b", "c", "d", "e", "f"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn binary_merge_presorted_flipped() {
        // Same as above, just flipped to ensure bulk copy from left.

        let left = generate_batch!([4, 5, 6], ["d", "e", "f"]);
        let right = generate_batch!([1, 2, 3], ["a", "b", "c"]);
        let out = binary_merge_exact_cap(&left, [0], &right, [0]);

        let expected = generate_batch!([1, 2, 3, 4, 5, 6], ["a", "b", "c", "d", "e", "f"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn binary_merge_almost_sorted() {
        let left = generate_batch!([1, 2, 6], ["a", "b", "c"]);
        let right = generate_batch!([3, 4, 5], ["d", "e", "f"]);
        let out = binary_merge_exact_cap(&left, [0], &right, [0]);

        let expected = generate_batch!([1, 2, 3, 4, 5, 6], ["a", "b", "d", "e", "f", "c"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn binary_merge_heap_key_inline() {
        // Sort keys are the strings, however all strings can be inlined so no
        // actual need to check the heap blocks.

        let left = generate_batch!([100, 2, 60], ["a", "b", "f"]);
        let right = generate_batch!([32, 40, 5], ["c", "d", "e"]);
        let out = binary_merge_exact_cap(&left, [1], &right, [1]);

        let expected = generate_batch!([100, 2, 32, 40, 5, 60], ["a", "b", "c", "d", "e", "f"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn binary_merge_heap_key_prefix_check() {
        // Sort keys are the strings that do spill to heap, but the prefix is
        // enough to sort them.

        let left = generate_batch!(
            [100, 2, 60],
            [
                "a$$$$$$$$$$$$$$$$$$$$",
                "b$$$$$$$$$$$$$$$$$$$$",
                "f$$$$$$$$$$$$$$$$$$$$"
            ]
        );
        let right = generate_batch!(
            [32, 40, 5],
            [
                "c$$$$$$$$$$$$$$$$$$$$",
                "d$$$$$$$$$$$$$$$$$$$$",
                "e$$$$$$$$$$$$$$$$$$$$"
            ]
        );
        let out = binary_merge_exact_cap(&left, [1], &right, [1]);

        let expected = generate_batch!(
            [100, 2, 32, 40, 5, 60],
            [
                "a$$$$$$$$$$$$$$$$$$$$",
                "b$$$$$$$$$$$$$$$$$$$$",
                "c$$$$$$$$$$$$$$$$$$$$",
                "d$$$$$$$$$$$$$$$$$$$$",
                "e$$$$$$$$$$$$$$$$$$$$",
                "f$$$$$$$$$$$$$$$$$$$$"
            ]
        );
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn binary_merge_heap_key_check_heap() {
        // Sort keys are the strings that do spill to heap, and we need to read
        // the heap to compare.

        let left = generate_batch!(
            [100, 2, 60],
            [
                "$$$$$$$$$$$$$$$$$$$$a",
                "$$$$$$$$$$$$$$$$$$$$b",
                "$$$$$$$$$$$$$$$$$$$$f"
            ]
        );
        let right = generate_batch!(
            [32, 40, 5],
            [
                "$$$$$$$$$$$$$$$$$$$$c",
                "$$$$$$$$$$$$$$$$$$$$d",
                "$$$$$$$$$$$$$$$$$$$$e"
            ]
        );
        let out = binary_merge_exact_cap(&left, [1], &right, [1]);

        let expected = generate_batch!(
            [100, 2, 32, 40, 5, 60],
            [
                "$$$$$$$$$$$$$$$$$$$$a",
                "$$$$$$$$$$$$$$$$$$$$b",
                "$$$$$$$$$$$$$$$$$$$$c",
                "$$$$$$$$$$$$$$$$$$$$d",
                "$$$$$$$$$$$$$$$$$$$$e",
                "$$$$$$$$$$$$$$$$$$$$f"
            ]
        );
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn binary_merge_left_right_exceeds_block_cap() {
        // Test case where the left and right blocks exceed the output block
        // size, resulting in an output segment containing multiple blocks.
        //
        // Addresses: <https://github.com/GlareDB/glaredb/issues/3733>
        //
        // LEFT:       4 rows
        // RIGHT:      2 rows
        // OUTPUT CAP: 4 rows

        let left = generate_batch!([8, 4, 5, 2]);
        let right = generate_batch!([9, 6]);

        let left_block = TestSortedRowBlock::from_batch(&left, [0]);
        let right_block = TestSortedRowBlock::from_batch(&right, [0]);

        let left_run = SortedSegment::from_sorted_block(left_block.sorted_block);
        let right_run = SortedSegment::from_sorted_block(right_block.sorted_block);

        let merger = BinaryMerger::new(
            &DefaultBufferManager,
            &left_block.key_layout,
            &left_block.data_layout,
            4,
        );
        let mut state = merger.init_merge_state();
        let out = merger.merge(&mut state, left_run, right_run, None).unwrap();
        assert_eq!(2, out.keys.len());

        let mut scan = out.init_scan_state();
        let mut out_batch = Batch::new([DataType::int32()], 4).unwrap();
        out.scan_data(&mut scan, &left_block.data_layout, &mut out_batch)
            .unwrap();

        // First block
        let expected1 = generate_batch!([2, 4, 5, 6]);
        assert_batches_eq(&expected1, &out_batch);

        out.scan_data(&mut scan, &left_block.data_layout, &mut out_batch)
            .unwrap();

        // Second block
        let expected2 = generate_batch!([8, 9]);
        assert_batches_eq(&expected2, &out_batch);
    }

    #[test]
    fn binary_merge_left_runs_out() {
        // Test case where the left segment runs out of blocks much earlier than
        // the right segment, making the inner loop need to scan right multiple
        // times while left is out of rows.
        //
        // We trigger this by creating two sorted runs (via the merger) where
        // the left run will exhuast first.

        // Batches merged for the first sorted run.
        let left1 = generate_batch!([1, 3]);
        let left2 = generate_batch!([2, 4]);

        let left_block1 = TestSortedRowBlock::from_batch(&left1, [0]);
        let left_block2 = TestSortedRowBlock::from_batch(&left2, [0]);

        let merger = BinaryMerger::new(
            &DefaultBufferManager,
            &left_block1.key_layout,
            &left_block1.data_layout,
            2,
        );

        let left_run1 = SortedSegment::from_sorted_block(left_block1.sorted_block);
        let left_run2 = SortedSegment::from_sorted_block(left_block2.sorted_block);

        let mut state = merger.init_merge_state();
        let left_out = merger
            .merge(&mut state, left_run1, left_run2, None)
            .unwrap();
        assert_eq!(2, left_out.keys.len());

        // Now do the same with some right blocks, these values always sort
        // higher than the left blocks.
        let right1 = generate_batch!([5, 7]);
        let right2 = generate_batch!([6, 8]);

        let right_block1 = TestSortedRowBlock::from_batch(&right1, [0]);
        let right_block2 = TestSortedRowBlock::from_batch(&right2, [0]);

        let right_run1 = SortedSegment::from_sorted_block(right_block1.sorted_block);
        let right_run2 = SortedSegment::from_sorted_block(right_block2.sorted_block);

        let right_out = merger
            .merge(&mut state, right_run1, right_run2, None)
            .unwrap();
        assert_eq!(2, right_out.keys.len());

        // Now merge the two sorted runs. Left with exhaust quicker than right
        // when producing the final run.

        let final_out = merger.merge(&mut state, left_out, right_out, None).unwrap();
        assert_eq!(4, final_out.keys.len());

        let mut scan = final_out.init_scan_state();
        let mut out_batch = Batch::new([DataType::int32()], 2).unwrap();
        let mut assert_scan = |expected: Batch| {
            final_out
                .scan_data(&mut scan, &left_block1.data_layout, &mut out_batch)
                .unwrap();

            assert_batches_eq(&expected, &out_batch);
        };

        // First batch.
        assert_scan(generate_batch!([1, 2]));
        // Second
        assert_scan(generate_batch!([3, 4]));
        // Third
        assert_scan(generate_batch!([5, 6]));
        // Fourth
        assert_scan(generate_batch!([7, 8]));
    }

    #[test]
    fn binary_merge_fewer_blocks_on_left() {
        let left = generate_batch!([1, 9]);
        let left_block = TestSortedRowBlock::from_batch(&left, [0]);

        let left_run = SortedSegment::from_sorted_block(left_block.sorted_block);

        let merger = BinaryMerger::new(
            &DefaultBufferManager,
            &left_block.key_layout,
            &left_block.data_layout,
            2,
        );

        let right1 = generate_batch!([5, 7]);
        let right2 = generate_batch!([6, 8]);

        let right_block1 = TestSortedRowBlock::from_batch(&right1, [0]);
        let right_block2 = TestSortedRowBlock::from_batch(&right2, [0]);

        let right_run1 = SortedSegment::from_sorted_block(right_block1.sorted_block);
        let right_run2 = SortedSegment::from_sorted_block(right_block2.sorted_block);

        let mut state = merger.init_merge_state();
        let right_out = merger
            .merge(&mut state, right_run1, right_run2, None)
            .unwrap();
        assert_eq!(2, right_out.keys.len());

        let final_out = merger.merge(&mut state, left_run, right_out, None).unwrap();
        assert_eq!(3, final_out.keys.len());

        let mut scan = final_out.init_scan_state();
        let mut out_batch = Batch::new([DataType::int32()], 2).unwrap();
        let mut assert_scan = |expected: Batch| {
            final_out
                .scan_data(&mut scan, &left_block.data_layout, &mut out_batch)
                .unwrap();

            assert_batches_eq(&expected, &out_batch);
        };

        assert_scan(generate_batch!([1, 5]));
        assert_scan(generate_batch!([6, 7]));
        assert_scan(generate_batch!([8, 9]));
    }

    #[test]
    fn binary_merge_interleave_multiple_blocks() {
        // Batches merged for the first sorted run.
        let left1 = generate_batch!([1, 2]);
        let left2 = generate_batch!([4, 5]);

        let left_block1 = TestSortedRowBlock::from_batch(&left1, [0]);
        let left_block2 = TestSortedRowBlock::from_batch(&left2, [0]);

        let merger = BinaryMerger::new(
            &DefaultBufferManager,
            &left_block1.key_layout,
            &left_block1.data_layout,
            2,
        );

        let left_run1 = SortedSegment::from_sorted_block(left_block1.sorted_block);
        let left_run2 = SortedSegment::from_sorted_block(left_block2.sorted_block);

        let mut state = merger.init_merge_state();
        let left_out = merger
            .merge(&mut state, left_run1, left_run2, None)
            .unwrap();
        assert_eq!(2, left_out.keys.len());

        let right1 = generate_batch!([3, 6]);
        let right2 = generate_batch!([7, 8]);

        let right_block1 = TestSortedRowBlock::from_batch(&right1, [0]);
        let right_block2 = TestSortedRowBlock::from_batch(&right2, [0]);

        let right_run1 = SortedSegment::from_sorted_block(right_block1.sorted_block);
        let right_run2 = SortedSegment::from_sorted_block(right_block2.sorted_block);

        let mut state = merger.init_merge_state();
        let right_out = merger
            .merge(&mut state, right_run1, right_run2, None)
            .unwrap();
        assert_eq!(2, right_out.keys.len());

        let final_out = merger.merge(&mut state, left_out, right_out, None).unwrap();
        assert_eq!(4, final_out.keys.len());

        let mut scan = final_out.init_scan_state();
        let mut out_batch = Batch::new([DataType::int32()], 2).unwrap();
        let mut assert_scan = |expected: Batch| {
            final_out
                .scan_data(&mut scan, &left_block1.data_layout, &mut out_batch)
                .unwrap();

            assert_batches_eq(&expected, &out_batch);
        };

        assert_scan(generate_batch!([1, 2]));
        assert_scan(generate_batch!([3, 4]));
        assert_scan(generate_batch!([5, 6]));
        assert_scan(generate_batch!([7, 8]));
    }

    #[test]
    fn binary_merge_blocks_should_not_exceed_capacity() {
        let left = generate_batch!([8, 4, 5, 2, 10, 11]);
        let right = generate_batch!([9, 6, 5, 3]);

        let left_block = TestSortedRowBlock::from_batch(&left, [0]);
        let right_block = TestSortedRowBlock::from_batch(&right, [0]);

        let left_run = SortedSegment::from_sorted_block(left_block.sorted_block);
        let right_run = SortedSegment::from_sorted_block(right_block.sorted_block);

        let merger = BinaryMerger::new(
            &DefaultBufferManager,
            &left_block.key_layout,
            &left_block.data_layout,
            4,
        );

        let mut state = merger.init_merge_state();
        let out = merger.merge(&mut state, left_run, right_run, None).unwrap();
        assert_eq!(3, out.keys.len());

        assert_eq!(4, out.keys[0].num_rows(left_block.key_layout.row_width));
        assert_eq!(4, out.keys[1].num_rows(left_block.key_layout.row_width));
        assert_eq!(2, out.keys[2].num_rows(left_block.key_layout.row_width));
    }

    #[test]
    fn binary_merge_multiple_left_blocks_larger_than_cap_right_block() {
        let left_batch = generate_batch!(0..2048);
        let left_block = TestSortedRowBlock::from_batch(&left_batch, [0]);
        let mut left_run = SortedSegment::from_sorted_block(left_block.sorted_block);

        let merger = BinaryMerger::new(
            &DefaultBufferManager,
            &left_block.key_layout,
            &left_block.data_layout,
            2048,
        );

        // Add additional blocks.
        for _ in 0..5 {
            let append_batch = generate_batch!(0..2048);
            let append_block = TestSortedRowBlock::from_batch(&append_batch, [0]);
            let append_run = SortedSegment::from_sorted_block(append_block.sorted_block);

            let mut state = merger.init_merge_state();
            left_run = merger
                .merge(&mut state, left_run, append_run, None)
                .unwrap();
        }

        // Now create a larger than normal right block/run
        let right_batch = generate_batch!(0..2117);
        let right_block = TestSortedRowBlock::from_batch(&right_batch, [0]);
        let right_run = SortedSegment::from_sorted_block(right_block.sorted_block);

        // Merge them.
        let mut state = merger.init_merge_state();
        let out = merger.merge(&mut state, left_run, right_run, None).unwrap();
        assert_eq!(8, out.keys.len());

        assert_eq!(2048, out.keys[0].num_rows(left_block.key_layout.row_width));
        assert_eq!(2048, out.keys[1].num_rows(left_block.key_layout.row_width));
        assert_eq!(2048, out.keys[2].num_rows(left_block.key_layout.row_width));
        assert_eq!(2048, out.keys[3].num_rows(left_block.key_layout.row_width));
        assert_eq!(2048, out.keys[4].num_rows(left_block.key_layout.row_width));
        assert_eq!(2048, out.keys[5].num_rows(left_block.key_layout.row_width));
        assert_eq!(2048, out.keys[6].num_rows(left_block.key_layout.row_width));
        assert_eq!(69, out.keys[7].num_rows(left_block.key_layout.row_width));
    }
}
