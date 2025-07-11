use std::borrow::{Borrow, BorrowMut};

use glaredb_error::Result;

use super::aggregate_layout::AggregateLayout;
use super::block::ValidityInitializer;
use super::row_blocks::{BlockAppendState, RowBlocks};
use super::row_scan::RowScanState;
use crate::arrays::array::Array;
use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::util::iter::IntoExactSizeIterator;

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
    /// Aggregate layout that all rows conform to.
    layout: AggregateLayout,
    /// Underlying row blocks storing both groups and states.
    blocks: RowBlocks<ValidityInitializer>,
}

impl AggregateCollection {
    pub fn new(layout: AggregateLayout, block_capacity: usize) -> Self {
        let blocks = RowBlocks::new(
            &DefaultBufferManager,
            ValidityInitializer::from_aggregate_layout(&layout),
            layout.row_width,
            block_capacity,
            Some(layout.base_align),
        );

        AggregateCollection { layout, blocks }
    }

    pub fn init_append_state(&self) -> AggregateAppendState {
        AggregateAppendState {
            block_append: BlockAppendState {
                row_pointers: Vec::new(),
                heap_pointers: Vec::new(),
            },
            heap_sizes: Vec::new(),
        }
    }

    pub fn row_mut_ptr_iter(&self) -> impl Iterator<Item = *mut u8> + '_ {
        self.blocks.row_mut_ptr_iter()
    }

    /// Get the total number of groups stored in this collection.
    pub fn num_groups(&self) -> usize {
        self.blocks.total_rows()
    }

    pub fn num_row_blocks(&self) -> usize {
        self.blocks.num_row_blocks()
    }

    pub(crate) fn row_blocks(&self) -> &RowBlocks<ValidityInitializer> {
        &self.blocks
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
        for (offset, agg) in self.layout.iter_offsets_and_aggregates() {
            for row_ptr in &state.block_append.row_pointers {
                // SAFETY: Construction of this collection should have asserted
                // that the function impls correspond to the aggregate layout.
                //
                // If that holds, these offsets should all be in bounds and be
                // well-aligned for each aggregate's state.
                unsafe {
                    let state_ptr = row_ptr.byte_add(offset);
                    agg.function.call_new_aggregate_state(state_ptr)
                }
            }
        }

        Ok(())
    }

    /// Scans all groups from the collection.
    pub fn scan_groups<A>(
        &self,
        state: &mut RowScanState,
        outputs: &mut [A],
        count: usize,
    ) -> Result<usize>
    where
        A: BorrowMut<Array>,
    {
        state.scan(&self.layout.groups, &self.blocks, outputs, count)
    }

    /// Scan a subset of the groups.
    pub fn scan_groups_subset<A>(
        &self,
        state: &mut RowScanState,
        columns: impl IntoExactSizeIterator<Item = usize> + Clone,
        outputs: &mut [A],
        count: usize,
    ) -> Result<usize>
    where
        A: BorrowMut<Array>,
    {
        state.scan_subset(&self.layout.groups, &self.blocks, columns, outputs, count)
    }

    #[allow(unused)] // Useful for tests.
    pub(crate) unsafe fn finalize_groups<A>(
        &self,
        group_ptrs: &mut [*mut u8],
        groups: &mut [A],
        results: &mut [A],
    ) -> Result<()>
    where
        A: BorrowMut<Array>,
    {
        unsafe {
            debug_assert_eq!(groups.len(), self.layout.groups.num_columns());
            debug_assert_eq!(results.len(), self.layout.aggregates.len());

            // Read out groups first. Pointer should aready point to the right
            // spot.
            {
                let group_ptrs = group_ptrs.iter().copied().map(|ptr| ptr as _);
                self.layout
                    .groups
                    .read_arrays(group_ptrs, groups.iter_mut().enumerate(), 0)?;
            }

            // Now read out the results from the aggreate. This modifies pointers.
            self.layout.finalize_states(group_ptrs, results)?;

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::arrays::row::aggregate_layout::AggregateUpdateSelector;
    use crate::expr::physical::PhysicalAggregateExpression;
    use crate::expr::{self, bind_aggregate_function};
    use crate::functions::aggregate::builtin::sum::FUNCTION_SET_SUM;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn append_groups_finalize_no_update() {
        // Create two groups, then finalize both groups. No updates, so the
        // results should be the "empty" value for the agg.

        // GROUP     (col0): Utf8
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::int64())],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::int64())],
        )];

        let layout = AggregateLayout::try_new([DataType::utf8()], aggs).unwrap();

        let mut collection = AggregateCollection::new(layout, 16);
        let mut state = collection.init_append_state();
        collection
            .append_groups(
                &mut state,
                &[Array::try_from_iter(["group_a", "group_b"]).unwrap()],
                0..2,
            )
            .unwrap();

        // No updates, just finalize. Assert that state was initialized and
        // produces a valid "empty" value.
        let mut ptrs = state.row_pointers().to_vec();

        let mut groups = Array::new(&DefaultBufferManager, DataType::utf8(), 2).unwrap();
        let mut results = Array::new(&DefaultBufferManager, DataType::int64(), 2).unwrap();

        unsafe {
            collection
                .finalize_groups(&mut ptrs, &mut [&mut groups], &mut [&mut results])
                .unwrap();
        }

        let expected_groups = Array::try_from_iter(["group_a", "group_b"]).unwrap();
        let expected_results = Array::try_from_iter([None as Option<i64>, None]).unwrap();

        assert_arrays_eq(&expected_groups, &groups);
        assert_arrays_eq(&expected_results, &results);
    }

    #[test]
    fn append_groups_finalize_with_update() {
        // Same as above, but we do an update to the aggregate.

        // GROUP     (col0): Utf8
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::int64())],
        )
        .unwrap();
        let aggs = [PhysicalAggregateExpression::new(
            sum_agg,
            [(1, DataType::int64())],
        )];

        let layout = AggregateLayout::try_new([DataType::utf8()], aggs).unwrap();

        let mut collection = AggregateCollection::new(layout, 16);
        let mut state = collection.init_append_state();
        collection
            .append_groups(
                &mut state,
                &[Array::try_from_iter(["group_a", "group_b"]).unwrap()],
                0..2,
            )
            .unwrap();

        // Update aggregates, groups alternating between rows.
        let ptrs = state.row_pointers();
        let mut update_row_ptrs = vec![ptrs[0], ptrs[1], ptrs[0], ptrs[1]];
        let values = Array::try_from_iter([1_i64, 2, 3, 4]).unwrap();

        unsafe {
            collection
                .layout
                .update_states(
                    &mut update_row_ptrs,
                    [AggregateUpdateSelector {
                        aggregate_idx: 0,
                        inputs: &[values],
                    }],
                    4,
                )
                .unwrap();
        }

        let mut ptrs = state.row_pointers().to_vec();

        let mut groups = Array::new(&DefaultBufferManager, DataType::utf8(), 2).unwrap();
        let mut results = Array::new(&DefaultBufferManager, DataType::int64(), 2).unwrap();

        unsafe {
            collection
                .finalize_groups(&mut ptrs, &mut [&mut groups], &mut [&mut results])
                .unwrap();
        }

        let expected_groups = Array::try_from_iter(["group_a", "group_b"]).unwrap();
        let expected_results = Array::try_from_iter([4_i64, 6]).unwrap();

        assert_arrays_eq(&expected_groups, &groups);
        assert_arrays_eq(&expected_results, &results);
    }
}
