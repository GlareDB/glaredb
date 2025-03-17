use glaredb_error::Result;

use super::sort_layout::SortLayout;
use crate::arrays::row::block::Block;
use crate::arrays::row::block_scan::BlockScanState;
use crate::arrays::row::row_layout::RowLayout;
use crate::buffer::buffer_manager::AsRawBufferManager;
use crate::util::iter::IntoExactSizeIterator;

/// Contains a single block with all keys in sorted order.
#[derive(Debug)]
pub struct SortedBlock {
    /// All sort keys in this block.
    ///
    /// Encoded using a sort layout.
    pub(crate) keys: Block,
    /// Keys that also require the heap.
    ///
    /// Encoded using a row layout.
    pub(crate) heap_keys: Block,
    /// Heap blocks for `heap_keys`.
    pub(crate) heap_keys_heap: Vec<Block>,
    /// Data not part of the sort keys.
    pub(crate) data: Block,
    /// Heap blocks for data.
    pub(crate) data_heap: Vec<Block>,
}

impl SortedBlock {
    /// Create a sorted block from the given key/data blocks.
    ///
    /// Returns None if the number of rows in the block is zero.
    #[allow(clippy::too_many_arguments)] // I know
    pub fn sort_from_blocks(
        manager: &impl AsRawBufferManager,
        key_layout: &SortLayout,
        data_layout: &RowLayout,
        mut keys: Block,
        heap_keys: Block,
        heap_keys_heap: Vec<Block>,
        data: Block,
        data_heap: Vec<Block>,
    ) -> Result<Option<Self>> {
        let row_width = key_layout.row_width;
        let num_rows = keys.num_rows(row_width);

        if num_rows == 0 {
            // Mostly just makes the below stuff easier.
            return Ok(None);
        }

        // Write row index to each row.
        let keys_ptr = keys.as_mut_ptr();
        for row_idx in 0..num_rows {
            unsafe {
                let row_idx_ptr = keys_ptr
                    .byte_add(key_layout.row_width * row_idx + key_layout.compare_width)
                    .cast::<u32>();
                row_idx_ptr.write_unaligned(row_idx as u32);
            }
        }

        let mut compare_offset = 0;
        let mut sort_width = 0;
        let mut first_sort = true;
        // Indicate if row at index 'i' is tied with row at index 'i+1'.
        let mut tied_with_next = vec![true; num_rows - 1];

        for col_idx in 0..key_layout.num_columns() {
            sort_width += key_layout.column_widths[col_idx];

            // Continue to add to sort width while we haven't reached a column
            // that requires us to look at the heap. This just lets us do fewere
            // comparisons.
            if col_idx < key_layout.num_columns() - 1 && !key_layout.column_requires_heap(col_idx) {
                continue;
            }

            if first_sort {
                // First sort, start with sorting all rows in the block.
                unsafe {
                    sort_keys_in_place(
                        manager,
                        key_layout,
                        &mut keys,
                        0,
                        num_rows,
                        compare_offset,
                        sort_width,
                    )?;
                }

                first_sort = false;
            } else {
                unsafe {
                    sort_tied_keys_in_place(
                        manager,
                        key_layout,
                        &mut keys,
                        &tied_with_next,
                        compare_offset,
                        sort_width,
                    )?;
                }
            }

            // All columns sorted and none require checking the heap, we're
            // done after the first sort.
            if col_idx == key_layout.num_columns() - 1 && !key_layout.column_requires_heap(col_idx)
            {
                break;
            }

            // Find subsequent rows that are tied with eachother.
            unsafe {
                fill_ties(
                    key_layout,
                    &keys,
                    compare_offset,
                    sort_width,
                    &mut tied_with_next,
                );
            }

            if tied_with_next.iter().all(|&v| !v) {
                // No ties, we're done.
                break;
            }

            // TODO: Sort (non-inline) blobs

            compare_offset += sort_width;
            sort_width = 0;
        }

        // Reorder heap keys and data according to the sorted keys.
        //
        // Note this doesn't reorder heap blocks as we have active pointers to
        // those from the row blocks.
        let mut sorted_heap_keys =
            Block::try_new_reserve_none(manager, heap_keys.reserved_bytes, None)?;
        if key_layout.any_requires_heap() {
            sorted_heap_keys.reserved_bytes = heap_keys.reserved_bytes;
            let row_idx_iter = BlockRowIndexIter::new(key_layout, &keys, num_rows);
            apply_sort_indices(
                &heap_keys,
                &mut sorted_heap_keys,
                row_idx_iter,
                key_layout.heap_layout.row_width,
            );
        }

        // Apply sort indices to the data blocks.
        let mut sorted_data = Block::try_new_reserve_none(manager, data.reserved_bytes, None)?;
        if data_layout.num_columns() > 0 {
            sorted_data.reserved_bytes = data.reserved_bytes;
            let row_idx_iter = BlockRowIndexIter::new(key_layout, &keys, num_rows);
            apply_sort_indices(&data, &mut sorted_data, row_idx_iter, data_layout.row_width);
        }

        Ok(Some(SortedBlock {
            keys,
            heap_keys: sorted_heap_keys,
            heap_keys_heap,
            data: sorted_data,
            data_heap,
        }))
    }

    /// Prepares the read state for reading the _data_ block for this sorted row
    /// block.
    ///
    /// State is cleared prior to pushing pointers.
    ///
    /// # Safety
    ///
    /// - The row layout passed in must represent same layout that was used when
    ///   creating the block.
    /// - The selection must provide valid row indices for initialized rows.
    #[allow(unused)] // Useful for tests, try to remove
    pub(crate) unsafe fn prepare_data_read(
        &self,
        state: &mut BlockScanState,
        data_layout: &RowLayout,
        selection: impl IntoIterator<Item = usize>,
    ) -> Result<()> {
        unsafe {
            state.prepare_block_scan(&self.data, data_layout.row_width, selection, true);
            Ok(())
        }
    }
}

/// Compares subquent rows for tied values and writes the result to `ties`.
///
/// Skips rows that have been determined to not be tied.
unsafe fn fill_ties(
    layout: &SortLayout,
    block: &Block,
    compare_offset: usize,
    compare_width: usize,
    ties: &mut [bool],
) {
    unsafe {
        let mut col_ptr = block.as_ptr();
        col_ptr = col_ptr.byte_add(compare_offset);

        // Note that `ties` is 1 less than the number of rows we're checking.
        for tie in ties {
            if !*tie {
                continue;
            }

            let a_ptr = col_ptr;
            let b_ptr = a_ptr.byte_add(layout.row_width);

            let a = std::slice::from_raw_parts(a_ptr, compare_width);
            let b = std::slice::from_raw_parts(b_ptr, compare_width);

            col_ptr = b_ptr;

            *tie = a == b
        }
    }
}

unsafe fn sort_tied_keys_in_place(
    manager: &impl AsRawBufferManager,
    layout: &SortLayout,
    keys: &mut Block,
    ties: &[bool],
    compare_offset: usize,
    compare_width: usize,
) -> Result<()> {
    let mut row_offset = 0;

    while row_offset < ties.len() {
        if !ties[row_offset] {
            // Row not tied with the next.
            row_offset += 1;
            continue;
        }

        // Row is tied with the next. Find all contiguously tied rows.
        let mut next_row_offset = row_offset + 1;
        while next_row_offset < ties.len() {
            if ties[row_offset] {
                // Not tied with us, don't include in the sort.
                break;
            }
            next_row_offset += 1;
        }

        let row_count = next_row_offset - row_offset + 1;

        // Sort the subset.
        unsafe {
            sort_keys_in_place(
                manager,
                layout,
                keys,
                row_offset,
                row_count,
                compare_offset,
                compare_width,
            )?;
        }

        row_offset = next_row_offset
    }

    Ok(())
}

/// Sorts a contiguous slice of keys in the keys blocks.
unsafe fn sort_keys_in_place(
    manager: &impl AsRawBufferManager,
    layout: &SortLayout,
    keys: &mut Block,
    row_offset: usize,
    row_count: usize,
    compare_offset: usize,
    compare_width: usize,
) -> Result<()> {
    debug_assert!(layout.row_width >= compare_offset + compare_width);

    let start_ptr = keys.as_mut_ptr().byte_add(row_offset * layout.row_width);

    // Generate sorted indices for the subset of rows we're sorting.
    let mut sort_indices: Vec<_> = (0..row_count).collect();
    sort_indices.sort_unstable_by(|&a, &b| {
        let (a, b) = unsafe {
            let a_ptr = start_ptr.byte_add(a * layout.row_width + compare_offset);
            let b_ptr = start_ptr.byte_add(b * layout.row_width + compare_offset);

            // Note we're not producing a slice from the full row,
            // just the part that we want to compare.
            let a = std::slice::from_raw_parts(a_ptr, compare_width);
            let b = std::slice::from_raw_parts(b_ptr, compare_width);

            (a, b)
        };
        a.cmp(b)
    });

    // Copy rows into temp block in sort order.
    let mut temp_keys = Block::try_new_reserve_none(manager, layout.buffer_size(row_count), None)?;
    let mut temp_ptr = temp_keys.as_mut_ptr();

    for sort_idx in sort_indices {
        unsafe {
            let src_ptr = start_ptr.byte_add(sort_idx * layout.row_width);
            temp_ptr.copy_from_nonoverlapping(src_ptr, layout.row_width);
            temp_ptr = temp_ptr.byte_add(layout.row_width);
        }
    }

    // Now copy from the temp block back into the original keys block.
    let temp_ptr = temp_keys.as_ptr();
    unsafe {
        start_ptr.copy_from_nonoverlapping(temp_ptr, layout.row_width * row_count);
    }

    Ok(())
}

fn apply_sort_indices(
    src: &Block,
    dest: &mut Block,
    indices: impl IntoExactSizeIterator<Item = usize>,
    row_width: usize,
) {
    let indices = indices.into_exact_size_iter();

    debug_assert_eq!(src.data.capacity(), indices.len() * row_width);
    debug_assert_eq!(dest.data.capacity(), indices.len() * row_width);

    let src_ptr = src.as_ptr();
    let dest_ptr = dest.as_mut_ptr();

    for (dest_idx, src_idx) in indices.enumerate() {
        unsafe {
            let src_ptr = src_ptr.byte_add(row_width * src_idx);
            let dest_ptr = dest_ptr.byte_add(row_width * dest_idx);
            src_ptr.copy_to_nonoverlapping(dest_ptr, row_width);
        }
    }
}

/// Iterator over a block conforming to a sort layout for producing the row
/// indexes from the block.
///
/// Key blocks contain a row index at the end of each row. When we sort the key
/// blocks, those row indexes are included in the data that's moved. This iterator
/// serves to iterate over those reordered indices.
#[derive(Debug)]
struct BlockRowIndexIter<'a> {
    block_ptr: *const u8,
    count: usize,
    curr: usize,
    layout: &'a SortLayout,
}

impl<'a> BlockRowIndexIter<'a> {
    fn new(layout: &'a SortLayout, block: &Block, count: usize) -> Self {
        BlockRowIndexIter {
            block_ptr: block.as_ptr(),
            count,
            curr: 0,
            layout,
        }
    }
}

impl Iterator for BlockRowIndexIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr >= self.count {
            return None;
        }

        let val = unsafe {
            self.block_ptr
                .byte_add(self.layout.row_width * self.curr + self.layout.compare_width)
                .cast::<u32>()
                .read_unaligned() as usize
        };
        self.curr += 1;

        Some(val)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.count - self.curr;
        (rem, Some(rem))
    }
}

impl ExactSizeIterator for BlockRowIndexIter<'_> {}
