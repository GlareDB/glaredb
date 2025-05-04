use std::borrow::Borrow;

use glaredb_error::Result;

use super::sort_layout::SortLayout;
use crate::arrays::array::Array;
use crate::arrays::row::block::{NopInitializer, ValidityInitializer};
use crate::arrays::row::row_blocks::{BlockAppendState, RowBlocks};
use crate::util::iter::IntoExactSizeIterator;

#[derive(Debug)]
pub struct KeyEncodeAppendState {
    /// State for appending to row blocks for sort keys.
    key_block_append: BlockAppendState,
    /// State for appending row/heap blocks for columns in the sort key that
    /// need heap blocks.
    key_heap_block_append: BlockAppendState,
    /// Reusable buffer for computing heaps sizes needed per row.
    heap_sizes: Vec<usize>,
}

impl KeyEncodeAppendState {
    pub const fn empty() -> Self {
        KeyEncodeAppendState {
            key_block_append: BlockAppendState {
                row_pointers: Vec::new(),
                heap_pointers: Vec::new(),
            },
            key_heap_block_append: BlockAppendState {
                row_pointers: Vec::new(),
                heap_pointers: Vec::new(),
            },
            heap_sizes: Vec::new(),
        }
    }
}

/// Encodes sort key values.
///
/// All columns taking part in a sort will have fixed sized keys created.
///
/// Columns that are variable length (e.g. strings) will also be encoded using
/// the normal row layout to `key_heap_blocks`.
///
/// `row_selection` indicates which rows we'll be encoding.
pub fn sort_key_encode<A>(
    key_layout: &SortLayout,
    state: &mut KeyEncodeAppendState,
    key_blocks: &mut RowBlocks<NopInitializer>,
    key_heap_blocks: &mut RowBlocks<ValidityInitializer>,
    keys: &[A],
    row_selection: impl IntoExactSizeIterator<Item = usize> + Clone,
) -> Result<()>
where
    A: Borrow<Array>,
{
    let encode_count = row_selection.clone().into_exact_size_iter().len();

    // Encode sort keys first (no heap values)
    state.key_block_append.clear();
    key_blocks.prepare_append(&mut state.key_block_append, encode_count, None)?;
    debug_assert_eq!(
        0,
        key_blocks.num_heap_blocks(),
        "Fixed-size sort keys should not generate heap blocks"
    );

    unsafe {
        key_layout.write_key_arrays(&mut state.key_block_append, keys, encode_count)?;
    }

    // Encode sort keys that require heap blocks.
    if key_layout.any_requires_heap() {
        let heap_keys: Vec<&Array> = key_layout
            .heap_mapping
            .iter()
            .enumerate()
            .filter_map(|(key_idx, maybe_heap_idx)| {
                if maybe_heap_idx.is_some() {
                    Some(keys[key_idx].borrow())
                } else {
                    None
                }
            })
            .collect();

        // Compute heap sizes needed.
        state.heap_sizes.resize(encode_count, 0);
        key_layout.heap_layout.compute_heap_sizes(
            &heap_keys,
            row_selection.clone(),
            &mut state.heap_sizes,
        )?;

        state.key_heap_block_append.clear();
        key_heap_blocks.prepare_append(
            &mut state.key_heap_block_append,
            encode_count,
            Some(&state.heap_sizes),
        )?;

        unsafe {
            key_layout.heap_layout.write_arrays(
                &mut state.key_heap_block_append,
                &heap_keys,
                row_selection,
            )?;
        }
    }

    Ok(())
}
