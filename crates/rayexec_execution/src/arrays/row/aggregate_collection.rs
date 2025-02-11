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

/// Collects grouped aggregate data using a row layout.
#[derive(Debug)]
pub struct AggregateCollection {
    /// Aggregate function implementations.
    ///
    /// Used for aggregate state manipulation.
    ///
    /// The number of implemenations must match the number of aggregates as
    /// specified in the layout.
    function_impls: Vec<AggregateFunctionImpl>,
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
    /// This will initialize aggregate states for the newly appended rows.
    // TODO: Much of the function body is duplicated with `RowCollection`.
    pub fn append_groups<A>(
        &mut self,
        state: &mut AggregateAppendState,
        arrays: &[A],
        rows: impl IntoExactSizeIterator<Item = usize> + Clone,
    ) -> Result<()>
    where
        A: Borrow<Array>,
    {
        debug_assert_eq!(arrays.len(), self.layout.groups.num_columns());

        let num_rows = rows.clone().into_exact_size_iter().len();

        state.block_append.clear();
        if self.layout.groups.requires_heap {
            // Compute heap sizes per row.
            state.heap_sizes.resize(num_rows, 0);
            self.layout
                .groups
                .compute_heap_sizes(arrays, rows.clone(), &mut state.heap_sizes)?;
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
        unimplemented!()
        // unsafe {
        //     self.layout
        //         .groups
        //         .write_arrays(&mut state.block_append, arrays, num_rows)?
        // };

        // Ok(())
    }
}
