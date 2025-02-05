use rayexec_error::{RayexecError, Result};

use super::block::Block;
use super::row_layout::RowLayout;
use super::sort_layout::SortLayout;
use crate::arrays::array::buffer_manager::{BufferManager, NopBufferManager};

#[derive(Debug)]
pub struct SortedBlock<B: BufferManager> {
    /// All sort keys in this block.
    ///
    /// Encoded using a sort layout.
    keys: Block<B>,
    /// Keys that also require the heap.
    ///
    /// Encoded using a row layout.
    heap_keys: Block<B>,
    /// Heap blocks for `heap_keys`.
    heap_keys_heap: Vec<Block<B>>,
    /// Data not part of the sort keys.
    data: Block<B>,
    /// Heap blocks for data.
    data_heap: Vec<Block<B>>,
}

impl<B> SortedBlock<B>
where
    B: BufferManager,
{
    pub fn sort_from_blocks(
        manager: &B,
        key_layout: &SortLayout,
        data_layout: &RowLayout,
        keys: Block<B>,
        heap_keys: Block<B>,
        heap_keys_heap: Vec<Block<B>>,
        data: Block<B>,
        data_heap: Vec<Block<B>>,
    ) -> Result<Self> {
        let row_width = key_layout.row_width;
        let num_rows = keys.num_rows(row_width);

        if num_rows == 0 {
            // Mostly just makes the below stuff easier.
            return Err(RayexecError::new("Cannot sort zero rows"));
        }

        let mut sort_indices: Vec<_> = (0..num_rows).collect();
        let mut sort_width = 0;

        let mut first_sort = true;
        // Indicate if row at index 'i' is tied with row at index 'i+1'.
        let mut tied_with_next = vec![true; num_rows - 1];

        let mut out_keys = Block::try_new(manager, keys.reserved_bytes);

        for col_idx in 0..key_layout.num_columns() {
            sort_width += key_layout.column_widths[col_idx];

            // Continue to add to sort width while we haven't reached a column
            // that requires us to look at the heap. This just lets us do fewere
            // comparisons.
            if col_idx <= key_layout.num_columns() - 1 && key_layout.column_requires_heap(col_idx) {
                continue;
            }

            if first_sort {
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
            } else {
            }
        }

        unimplemented!()
    }
}

fn apply_sort_indices<B>(
    src: &mut Block<B>,
    dest: &mut Block<B>,
    indices: &[usize],
    row_width: usize,
) where
    B: BufferManager,
{
    debug_assert_eq!(src.data.capacity(), indices.len() * row_width);
    debug_assert_eq!(dest.data.capacity(), indices.len() * row_width);

    let src_ptr = src.as_ptr();
    let dest_ptr = dest.as_mut_ptr();

    for (dest_idx, &src_idx) in indices.iter().enumerate() {
        unsafe {
            let src_ptr = src_ptr.byte_add(row_width * src_idx);
            let dest_ptr = dest_ptr.byte_add(row_width * dest_idx);
            src_ptr.copy_to_nonoverlapping(dest_ptr, row_width);
        }
    }
}
