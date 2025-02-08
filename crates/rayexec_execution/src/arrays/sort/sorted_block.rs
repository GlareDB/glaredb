use rayexec_error::{RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

use super::sort_layout::SortLayout;
use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::row::block::Block;
use crate::arrays::row::block_scanner::BlockScanState;
use crate::arrays::row::row_layout::RowLayout;

/// Contains a single block with all keys in sorted order.
#[derive(Debug)]
pub struct SortedBlock<B: BufferManager> {
    /// All sort keys in this block.
    ///
    /// Encoded using a sort layout.
    pub(crate) keys: Block<B>,
    /// Keys that also require the heap.
    ///
    /// Encoded using a row layout.
    pub(crate) heap_keys: Block<B>,
    /// Heap blocks for `heap_keys`.
    pub(crate) heap_keys_heap: Vec<Block<B>>,
    /// Data not part of the sort keys.
    pub(crate) data: Block<B>,
    /// Heap blocks for data.
    pub(crate) data_heap: Vec<Block<B>>,
}

impl<B> SortedBlock<B>
where
    B: BufferManager,
{
    /// Create a sorted block from the given key/data blocks.
    ///
    /// Returns None if the number of rows in the block is zero.
    pub fn sort_from_blocks(
        manager: &B,
        key_layout: &SortLayout,
        data_layout: &RowLayout,
        mut keys: Block<B>,
        heap_keys: Block<B>,
        heap_keys_heap: Vec<Block<B>>,
        data: Block<B>,
        data_heap: Vec<Block<B>>,
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

        let mut sort_width = 0;
        let mut first_sort = true;
        // Indicate if row at index 'i' is tied with row at index 'i+1'.
        let mut tied_with_next = vec![true; num_rows - 1];

        let mut sorted_keys = Block::try_new_reserve_none(manager, keys.reserved_bytes)?;
        sorted_keys.reserved_bytes = keys.reserved_bytes;

        for col_idx in 0..key_layout.num_columns() {
            sort_width += key_layout.column_widths[col_idx];

            // Continue to add to sort width while we haven't reached a column
            // that requires us to look at the heap. This just lets us do fewere
            // comparisons.
            if col_idx < key_layout.num_columns() - 1 && !key_layout.column_requires_heap(col_idx) {
                continue;
            }

            if first_sort {
                let mut sort_indices: Vec<_> = (0..num_rows).collect();
                let keys_ptr = keys.as_ptr();

                sort_indices.sort_unstable_by(|&a, &b| {
                    let (a, b) = unsafe {
                        let a_ptr = keys_ptr.byte_add(a * row_width);
                        let b_ptr = keys_ptr.byte_add(b * row_width);

                        // Note we're not producing a slice from the full row,
                        // just the part that we want to compare.
                        let a = std::slice::from_raw_parts(a_ptr, sort_width);
                        let b = std::slice::from_raw_parts(b_ptr, sort_width);

                        (a, b)
                    };
                    a.cmp(b)
                });
                first_sort = false;

                // Apply first sort pass, writing to sorted keys block.
                apply_sort_indices(&keys, &mut sorted_keys, sort_indices, row_width);

                // All columns sorted and none require checking the heap, we're
                // done after the first sort.
                if col_idx == key_layout.num_columns() - 1
                    && !key_layout.column_requires_heap(col_idx)
                {
                    break;
                }
            } else {
            }

            unimplemented!()
        }

        // Reorder heap keys and data according to the sorted keys.
        //
        // Note this doesn't reorder heap blocks as we have active pointers to
        // those from the row blocks.
        let mut sorted_heap_keys = Block::try_new_reserve_none(manager, heap_keys.reserved_bytes)?;
        if key_layout.any_requires_heap() {
            sorted_heap_keys.reserved_bytes = heap_keys.reserved_bytes;
            let row_idx_iter = BlockRowIndexIter::new(&key_layout, &sorted_keys, num_rows);
            apply_sort_indices(
                &heap_keys,
                &mut sorted_heap_keys,
                row_idx_iter,
                key_layout.heap_layout.row_width,
            );
        }

        let mut sorted_data = Block::try_new_reserve_none(manager, data.reserved_bytes)?;
        if data_layout.num_columns() > 0 {
            sorted_data.reserved_bytes = data.reserved_bytes;
            let row_idx_iter = BlockRowIndexIter::new(&key_layout, &sorted_keys, num_rows);
            apply_sort_indices(&data, &mut sorted_data, row_idx_iter, data_layout.row_width);
        }

        Ok(Some(SortedBlock {
            keys: sorted_keys,
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
    pub(crate) unsafe fn prepare_data_read(
        &self,
        state: &mut BlockScanState,
        data_layout: &RowLayout,
        selection: impl IntoIterator<Item = usize>,
    ) -> Result<()> {
        state.prepare_block_scan(&self.data, data_layout.row_width, selection, true);
        Ok(())
    }
}

fn apply_sort_indices<B>(
    src: &Block<B>,
    dest: &mut Block<B>,
    indices: impl IntoExactSizeIterator<Item = usize>,
    row_width: usize,
) where
    B: BufferManager,
{
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
    fn new<B>(layout: &'a SortLayout, block: &Block<B>, count: usize) -> Self
    where
        B: BufferManager,
    {
        BlockRowIndexIter {
            block_ptr: block.as_ptr(),
            count,
            curr: 0,
            layout,
        }
    }
}

impl<'a> Iterator for BlockRowIndexIter<'a> {
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

impl<'a> ExactSizeIterator for BlockRowIndexIter<'a> {}
