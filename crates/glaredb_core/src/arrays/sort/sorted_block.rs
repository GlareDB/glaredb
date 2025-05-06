#![allow(clippy::needless_range_loop)] // Iter by index makes few thing in this clearer.

use glaredb_error::{Result, not_implemented};

use super::sort_layout::SortLayout;
use crate::arrays::bitmap::view::BitmapView;
use crate::arrays::row::block::Block;
use crate::arrays::row::block_scan::BlockScanState;
use crate::arrays::row::row_layout::RowLayout;
use crate::arrays::sort::heap_compare::{compare_heap_value_supported, compare_heap_values};
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
        limit_hint: Option<usize>,
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
        let mut compare_width = 0;
        let mut first_sort = true;
        // Indicate if row at index 'i' is tied with row at index 'i+1'.
        let mut tied_with_next = vec![true; num_rows - 1];

        for col_idx in 0..key_layout.num_columns() {
            compare_width += key_layout.column_widths[col_idx];

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
                        compare_width,
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
                        compare_width,
                    )?;
                }
            }

            // All columns sorted and none require checking the heap, we're
            // done after the first sort.
            if col_idx == key_layout.num_columns() - 1 && !key_layout.column_requires_heap(col_idx)
            {
                break;
            }

            // Find subsequent rows that are tied with each other.
            unsafe {
                fill_ties(
                    key_layout,
                    &keys,
                    compare_offset,
                    compare_width,
                    &mut tied_with_next,
                );
            }

            if tied_with_next.iter().all(|&v| !v) {
                // No ties, we're done.
                break;
            }

            // Sort heap keys if needed.
            if key_layout.heap_mapping[col_idx].is_some() {
                unsafe {
                    sort_tied_heap_keys_in_place(
                        manager,
                        key_layout,
                        &mut keys,
                        &heap_keys,
                        &heap_keys_heap,
                        &mut tied_with_next,
                        col_idx,
                    )?
                };

                // Sorting tied heap keys may have resolved our ties.
                if tied_with_next.iter().all(|&v| !v) {
                    // No ties, we're done.
                    break;
                }
            }

            compare_offset += compare_width;
            compare_width = 0;
        }

        // Number of rows to copy for block the heap keys, and the sorted data.
        let num_rows_to_copy = match limit_hint {
            Some(limit_hint) if limit_hint < num_rows => limit_hint,
            _ => num_rows,
        };
        // Block reserved capacities are what determines the number of
        // rows in block.
        //
        // This will just truncate the keys block if limit is less than the
        // number of encodeded keys.
        keys.reserved_bytes = key_layout.row_width * num_rows_to_copy;

        // Iter to read the first 'n' sorted rows. If we don't have a limit
        // hint, it'll just scan the entire block.
        let row_idx_iter = BlockRowIndexIter::new(key_layout, &keys, num_rows_to_copy);

        // Reorder heap keys and data according to the sorted keys.
        //
        // Note this doesn't reorder heap blocks as we have active pointers to
        // those from the row blocks.
        let key_heap_cap = num_rows_to_copy * key_layout.heap_layout.row_width;
        let mut sorted_heap_keys = Block::try_new_reserve_all(manager, key_heap_cap)?;
        if key_layout.any_requires_heap() {
            apply_sort_indices(
                &heap_keys,
                &mut sorted_heap_keys,
                row_idx_iter.clone(),
                key_layout.heap_layout.row_width,
            );
        }

        // Apply sort indices to the data blocks.
        let data_cap = data_layout.row_width * num_rows_to_copy;
        let mut sorted_data = Block::try_new_reserve_all(manager, data_cap)?;
        if data_layout.num_columns() > 0 {
            apply_sort_indices(&data, &mut sorted_data, row_idx_iter, data_layout.row_width);
        }

        debug_assert_eq!(num_rows_to_copy, keys.num_rows(key_layout.row_width));
        debug_assert!(num_rows_to_copy <= num_rows);

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

/// Reads the u32 row index value at the end of a row, and casts it to a usize
/// for convenience.
///
/// The pointer should be pointing to the start of a row located in a block with
/// a sort layout.
unsafe fn read_inline_idx(row_start: *const u8, layout: &SortLayout) -> usize {
    // TODO: Possibly require this to be aligned since we iterate over it.
    unsafe {
        row_start
            .byte_add(layout.compare_width)
            .cast::<u32>()
            .read_unaligned() as usize
    }
}

/// Get a row pointer for a row in a **fixed-size key block**.
unsafe fn key_row_ptr(key_block: &Block, layout: &SortLayout, row: usize) -> *const u8 {
    unsafe { key_block.as_ptr().byte_add(row * layout.row_width) }
}

/// Get a row pointer for a row in a **heap key block**.
unsafe fn heap_key_row_ptr(heap_key_block: &Block, layout: &SortLayout, row: usize) -> *const u8 {
    unsafe {
        heap_key_block
            .as_ptr()
            .byte_add(row * layout.heap_layout.row_width)
    }
}

/// Compares subsequent rows for tied values and writes the result to `ties`.
///
/// Each `ties[i]` is set to true if row[i] and row[i+1] are equal in the
/// compare range.
///
/// Assumes `ties.len() + 1 == row_count`.
unsafe fn fill_ties(
    layout: &SortLayout,
    block: &Block,
    compare_offset: usize,
    compare_width: usize,
    ties: &mut [bool],
) {
    let row_width = layout.row_width;
    let base_ptr = unsafe { block.as_ptr().add(compare_offset) };

    for i in 0..ties.len() {
        if !ties[i] {
            continue;
        }

        unsafe {
            let a_ptr = base_ptr.add(i * row_width);
            let b_ptr = base_ptr.add((i + 1) * row_width);

            let a = std::slice::from_raw_parts(a_ptr, compare_width);
            let b = std::slice::from_raw_parts(b_ptr, compare_width);

            ties[i] = a == b;
        }
    }
}

/// Sorts tied (fixed length) keys in place.
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
        while next_row_offset < ties.len() && ties[next_row_offset] {
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

    let start_ptr = unsafe { keys.as_mut_ptr().byte_add(row_offset * layout.row_width) };

    // Generate sorted indices for the subset of rows we're sorting.
    let mut sort_indices: Vec<_> = (0..row_count).collect();
    sort_indices.sort_unstable_by(|&a, &b| {
        let (a, b) = unsafe {
            let a_ptr = start_ptr.byte_add(a * layout.row_width + compare_offset);
            let b_ptr = start_ptr.byte_add(b * layout.row_width + compare_offset);

            // Note we're not producing a slice from the full row,
            // just the part that we want to compare.
            let a_slice = std::slice::from_raw_parts(a_ptr, compare_width);
            let b_slice = std::slice::from_raw_parts(b_ptr, compare_width);

            (a_slice, b_slice)
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

/// Sorts tied heap keys in place.
///
/// Unlike sorting for fixed size keys, this will also update `ties` to indicate
/// broken ties.
unsafe fn sort_tied_heap_keys_in_place(
    manager: &impl AsRawBufferManager,
    layout: &SortLayout,
    keys: &mut Block,
    heap_keys: &Block,
    heap_keys_heap: &[Block],
    ties: &mut [bool],
    key_column: usize,
) -> Result<()> {
    let mut row_offset = 0;

    while row_offset < ties.len() {
        if !ties[row_offset] {
            // Skip, row not tied.
            row_offset += 1;
            continue;
        }

        // Row is tied with the next. Find all contiguously tied rows.
        let mut next_row_offset = row_offset + 1;
        while next_row_offset < ties.len() && ties[next_row_offset] {
            next_row_offset += 1;
        }

        let row_count = next_row_offset - row_offset + 1;

        // Sort the subset.
        unsafe {
            sort_heap_keys(
                manager,
                layout,
                keys,
                heap_keys,
                heap_keys_heap,
                ties,
                key_column,
                row_offset,
                row_count,
            )?;
        }

        row_offset = next_row_offset;
    }

    Ok(())
}

/// Helper function for sorting **tied** heap keys.
#[allow(clippy::too_many_arguments)] // I know
unsafe fn sort_heap_keys(
    manager: &impl AsRawBufferManager,
    layout: &SortLayout,
    keys: &mut Block,
    heap_keys: &Block,
    _heap_keys_heap: &[Block],
    ties: &mut [bool],
    key_column: usize,
    row_offset: usize,
    row_count: usize,
) -> Result<()> {
    let heap_key_column =
        layout.heap_mapping[key_column].expect("to be provides s column that has a heap key");

    // Read the first row we're comparing to see if it's NULL. Since we're
    // comparing prefix ties, we should return early if the values are NULL.
    let first_heap_row_idx =
        unsafe { read_inline_idx(key_row_ptr(keys, layout, row_offset), layout) };
    let validity_buf = unsafe {
        layout
            .heap_layout
            .validity_buffer(heap_key_row_ptr(heap_keys, layout, first_heap_row_idx))
    };
    let valid =
        BitmapView::new(validity_buf, layout.heap_layout.num_columns()).value(heap_key_column);
    if !valid {
        // If the first row isn't valid, then all rows in this subset aren't
        // valid, as we're sorting ties. No sorting needs to be done.
        return Ok(());
    }

    // Generate sorted indices for the subset of rows we're sorting.
    // Relative to row offset.
    let mut sort_indices: Vec<_> = (0..row_count).collect();
    let phys_type = layout.heap_layout.types[heap_key_column].physical_type()?;
    if !compare_heap_value_supported(phys_type) {
        not_implemented!("Heap key compare for type {phys_type} (sorted block)")
    }

    let heap_col_offset = layout.heap_layout.offsets[heap_key_column];
    sort_indices.sort_unstable_by(|&a, &b| {
        // Note that heap key blocks are never reordered during
        // in-progress sorting, while fixed-len key blocks are. We need
        // to read the key block to get the original row for the heap
        // key block.
        let a_row_idx =
            unsafe { read_inline_idx(key_row_ptr(keys, layout, a + row_offset), layout) };
        let b_row_idx =
            unsafe { read_inline_idx(key_row_ptr(keys, layout, b + row_offset), layout) };

        // Use row indices to compute the pointers into heap key blocks.
        let a_ptr =
            unsafe { heap_key_row_ptr(heap_keys, layout, a_row_idx).byte_add(heap_col_offset) };
        let b_ptr =
            unsafe { heap_key_row_ptr(heap_keys, layout, b_row_idx).byte_add(heap_col_offset) };

        let ord =
            unsafe { compare_heap_values(a_ptr, b_ptr, phys_type).expect("supported phys type") };

        // Note we're using the original key column index.
        if layout.columns[key_column].desc {
            ord.reverse()
        } else {
            ord
        }
    });

    // Copy rows into temp block in sort order.
    //
    // We are only copying fixed-len sort keys here. Heap keys remain in
    // their original order.
    let mut temp_keys = Block::try_new_reserve_none(manager, layout.buffer_size(row_count), None)?;
    let mut temp_ptr = temp_keys.as_mut_ptr();

    for sort_idx in sort_indices {
        unsafe {
            let src_ptr = key_row_ptr(keys, layout, sort_idx + row_offset);
            temp_ptr.copy_from_nonoverlapping(src_ptr, layout.row_width);
            temp_ptr = temp_ptr.byte_add(layout.row_width);
        }
    }

    // Now copy from the temp block back into the original keys block.
    let temp_ptr = temp_keys.as_ptr();
    unsafe {
        let dest = key_row_ptr(keys, layout, row_offset).cast_mut();
        dest.copy_from_nonoverlapping(temp_ptr, layout.row_width * row_count);
    }

    // Now... Iterate throught he rows we just sorted to see if there's still
    // ties.
    let heap_curr_row_idx =
        unsafe { read_inline_idx(key_row_ptr(keys, layout, row_offset), layout) };
    let mut curr_heap_ptr =
        unsafe { heap_key_row_ptr(heap_keys, layout, heap_curr_row_idx).byte_add(heap_col_offset) };
    for next_row_idx in (row_offset + 1)..(row_offset + row_count) {
        let heap_next_row_idx =
            unsafe { read_inline_idx(key_row_ptr(keys, layout, next_row_idx), layout) };
        let next_heap_ptr = unsafe {
            heap_key_row_ptr(heap_keys, layout, heap_next_row_idx).byte_add(heap_col_offset)
        };

        // Compare curr with next.
        let cmp = unsafe { compare_heap_values(curr_heap_ptr, next_heap_ptr, phys_type)? };

        // Update ties for curr.
        ties[next_row_idx - 1] = cmp.is_eq();

        curr_heap_ptr = next_heap_ptr;
    }

    Ok(())
}

/// Applies sort indices to a src fixed-len block, writing the sorted output to
/// dest.
///
/// The indices iterator may contain fewer indices that what's in `src` if we
/// have a limit hint. The dest block will be exact sized to hold the output.
fn apply_sort_indices(
    src: &Block,
    dest: &mut Block,
    indices: impl IntoExactSizeIterator<Item = usize>,
    row_width: usize,
) {
    let indices = indices.into_exact_size_iter();

    // Indices may be few than src if we have a limit hint.
    debug_assert!(
        src.data.len() >= indices.len() * row_width,
        "src: {}, width {}",
        src.data.len(),
        indices.len() * row_width
    );
    // Output always exact sized, even with a limit hint.
    debug_assert_eq!(dest.data.len(), indices.len() * row_width);

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
#[derive(Debug, Clone)]
struct BlockRowIndexIter<'a> {
    keys: &'a Block,
    count: usize,
    curr: usize,
    layout: &'a SortLayout,
}

impl<'a> BlockRowIndexIter<'a> {
    fn new(layout: &'a SortLayout, keys: &'a Block, count: usize) -> Self {
        BlockRowIndexIter {
            keys,
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

        let idx =
            unsafe { read_inline_idx(key_row_ptr(self.keys, self.layout, self.curr), self.layout) };
        self.curr += 1;

        Some(idx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.count - self.curr;
        (rem, Some(rem))
    }
}

impl ExactSizeIterator for BlockRowIndexIter<'_> {}
