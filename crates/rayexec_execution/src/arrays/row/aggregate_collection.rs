use std::borrow::Borrow;

use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use super::aggregate_layout::AggregateLayout;
use super::block::ValidityInitializer;
use super::row_blocks::{BlockAppendState, RowBlocks};
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::Array;
use crate::functions::aggregate::states::AggregateFunctionImpl;

#[derive(Debug)]
pub struct AggregateAppendState {
    /// State for appending to row/heap blocks.
    block_append: BlockAppendState,
    /// Reusable buffer for computing heaps sizes needed per row.
    heap_sizes: Vec<usize>,
}

impl AggregateAppendState {
    /// Returns the row pointers from the most recent append to this collection.
    pub fn row_pointers(&self) -> &[*mut u8] {
        &self.block_append.row_pointers
    }
}

/// Collects grouped aggregate data using a row layout.
#[derive(Debug)]
pub struct AggregateCollection {
    /// Aggregate function implementations.
    ///
    /// Used for aggregate state manipulation.
    ///
    /// The number of implemenations must match the number of aggregates as
    /// specified in the layout.
    func_impls: Vec<AggregateFunctionImpl>,
    /// Aggregate layout that all rows conform to.
    layout: AggregateLayout,
    /// Underlying row blocks storing both groups and states.
    blocks: RowBlocks<NopBufferManager, ValidityInitializer>,
}

impl AggregateCollection {
    pub fn new(layout: AggregateLayout, block_capacity: usize) -> Self {
        unimplemented!()
    }

    /// Append new groups to the collection.
    ///
    /// `rows` selects the rows from the arrays that we should append to this
    /// collection.
    ///
    /// This will initialize aggregate states for the newly appended groups.
    // TODO: Much of the function body is duplicated with `RowCollection`.
    pub(crate) fn append_groups<A>(
        &mut self,
        state: &mut AggregateAppendState,
        groups: &[A],
        rows: impl IntoExactSizeIterator<Item = usize> + Clone,
    ) -> Result<()>
    where
        A: Borrow<Array>,
    {
        debug_assert_eq!(groups.len(), self.layout.groups.num_columns());

        let num_rows = rows.clone().into_exact_size_iter().len();

        state.block_append.clear();
        if self.layout.groups.requires_heap {
            // Compute heap sizes per row.
            state.heap_sizes.resize(num_rows, 0);
            self.layout
                .groups
                .compute_heap_sizes(groups, rows.clone(), &mut state.heap_sizes)?;
        }

        if self.layout.groups.requires_heap {
            self.blocks.prepare_append(
                &mut state.block_append,
                num_rows,
                Some(&state.heap_sizes),
            )?
        } else {
            self.blocks
                .prepare_append(&mut state.block_append, num_rows, None)?
        }

        // SAFETY: We assume that the pointers we computed are inbounds with
        // respect the blocks, and that we correctly computed the heap sizes.
        unsafe {
            self.layout
                .groups
                .write_arrays(&mut state.block_append, groups, rows)?
        };

        // Initialize aggregate states.
        for (&offset, func) in self.layout.aggregate_offsets.iter().zip(&self.func_impls) {
            let extra = func.extra_deref();

            for row_ptr in &state.block_append.row_pointers {
                // SAFETY: Construction of this collection should have asserted
                // that the function impls correspond to the aggregate layout.
                //
                // If that holds, these offsets should all be in bounds and be
                // well-aligned for each aggregate's state.
                unsafe {
                    let state_ptr = row_ptr.byte_add(offset);
                    (func.init_fn)(extra, state_ptr);
                }
            }
        }

        Ok(())
    }

    pub(crate) unsafe fn update_aggregates<A>(
        &mut self,
        row_ptrs: &[*mut u8],
        inputs: &[A],
        rows: impl IntoExactSizeIterator<Item = usize> + Clone,
    ) -> Result<()>
    where
        A: Borrow<Array>,
    {
        unimplemented!()
    }
}
