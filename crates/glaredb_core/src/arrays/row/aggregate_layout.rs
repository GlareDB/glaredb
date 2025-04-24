use std::borrow::BorrowMut;

use glaredb_error::Result;

use super::row_layout::RowLayout;
use crate::arrays::array::Array;
use crate::arrays::datatype::DataType;
use crate::expr::physical::PhysicalAggregateExpression;
use crate::util::iter::IntoExactSizeIterator;

/// Desribes the row layout for aggregate states and groups.
///
/// Holds the function implementations for how aggregates should create, update,
/// and finalize their states.
#[derive(Debug, Clone)]
pub struct AggregateLayout {
    /// Required base alignment for a buffer holding this aggregate layout.
    ///
    /// Every row will be aligned to this value which may mean that there exists
    /// padding at the end of a row.
    ///
    /// Every aggregate will by locally aligned within its row to allow for
    /// aligned reads and writes for in place updates.
    ///
    /// Group values are not guaranteed to be aligned, and should be
    /// written/read with bitwise copies.
    pub(crate) base_align: usize,
    /// Layout for the groups part of the aggregate.
    pub(crate) groups: RowLayout,
    /// Aggregates for this layout.
    // TODO: May be more than we want on the layout.
    pub(crate) aggregates: Vec<PhysicalAggregateExpression>,
    /// Row width in bytes of both the group values and the aggregates.
    pub(crate) row_width: usize,
    /// Byte offsets to the aggregates in the row.
    ///
    /// This is relative to the entire row width (both the group values and the
    /// aggregate states).
    ///
    /// These will be aligned to the aggregate object.
    pub(crate) aggregate_offsets: Vec<usize>,
}

#[derive(Debug)]
pub struct AggregateUpdateSelector<'a> {
    /// Index of the aggregate we're updating.
    pub aggregate_idx: usize,
    /// Inputs we're using to update the aggregate.
    ///
    /// This must be the exact number of inputs the aggregate is expecting.
    pub inputs: &'a [Array],
}

impl AggregateLayout {
    /// Create a new layout representing a row of group values and aggregates.
    pub fn new(
        group_types: impl IntoIterator<Item = DataType>,
        aggregates: impl IntoIterator<Item = PhysicalAggregateExpression>,
    ) -> Self {
        let groups = RowLayout::new(group_types);
        let aggregates: Vec<_> = aggregates.into_iter().collect();

        let base_align: usize = aggregates
            .iter()
            .map(|agg| agg.function.aggregate_state_info().align)
            .max()
            .unwrap_or(1);

        let offset = groups.row_width;
        let mut offset = align_len(offset, base_align);

        let mut aggregate_offsets = Vec::with_capacity(aggregates.len());

        for agg in &aggregates {
            aggregate_offsets.push(offset);
            let info = agg.function.aggregate_state_info();
            offset += info.size;
            // TODO: Could be more efficient here and align to the aggregate
            // itself.
            offset = align_len(offset, base_align);
        }

        // Ensure next row in the buffer matches up with the base alignment.
        let row_width = align_len(offset, base_align);

        AggregateLayout {
            base_align,
            groups,
            aggregates,
            row_width,
            aggregate_offsets,
        }
    }

    /// Return an iterator over relative offsets for each aggregate and the
    /// aggregate itself.
    pub fn iter_offsets_and_aggregates(
        &self,
    ) -> impl Iterator<Item = (usize, &'_ PhysicalAggregateExpression)> {
        debug_assert_eq!(self.aggregate_offsets.len(), self.aggregates.len());
        self.aggregate_offsets.iter().copied().zip(&self.aggregates)
    }

    pub fn iter_offsets_and_aggregates_selection(
        &self,
        agg_selection: impl IntoExactSizeIterator<Item = usize>,
    ) -> impl Iterator<Item = (usize, &'_ PhysicalAggregateExpression)> {
        debug_assert_eq!(self.aggregate_offsets.len(), self.aggregates.len());
        agg_selection
            .into_exact_size_iter()
            .map(|idx| (self.aggregate_offsets[idx], &self.aggregates[idx]))
    }

    /// Update aggregate states based on the inputs.
    ///
    /// `group_ptrs` points to the start of the rows that we should be updating.
    /// The row pointers will be modified to allow for updating multiple states
    /// at the same time. While row pointers will end up pointing to the last
    /// state in each row, this property should not be relied upon.
    ///
    /// `agg_updates` determins which aggregate values we're updating for these
    /// groups. The iterator must provide aggregates in order, and the number of
    /// inputs must be exact.
    ///
    /// The length of `group_ptrs` must equal `num_rows`.
    ///
    /// Note that we can't genericize `inputs` be either `&Array` or `Array`
    /// since the layout contains only function pointers (which don't accept
    /// generics).
    ///
    /// # Safety
    ///
    /// The pointers in `group_ptrs` must point to valid rows that match the
    /// layout of this collection.
    ///
    /// `group_ptrs` may contain duplicated pointers.
    pub(crate) unsafe fn update_states<'a>(
        &self,
        group_ptrs: &mut [*mut u8],
        agg_updates: impl IntoExactSizeIterator<Item = AggregateUpdateSelector<'a>>,
        num_rows: usize,
    ) -> Result<()> {
        // TODO: Where are the unit tests?

        debug_assert_eq!(num_rows, group_ptrs.len());

        let mut prev_offset = 0;

        for selector in agg_updates {
            // This aggregate was selected.
            let offset = self.aggregate_offsets[selector.aggregate_idx];
            let agg = &self.aggregates[selector.aggregate_idx];

            // Update pointers to point to the start of this aggregate's state.
            //
            // We compute the offset relative to where the pointer is right now
            // instead of using the offset from the start of the row since we
            // update the row pointer for each aggregate we're updating.
            let rel_offset = offset - prev_offset;
            for row_ptr in group_ptrs.iter_mut() {
                *row_ptr = unsafe { row_ptr.byte_add(rel_offset) };
                debug_assert_eq!(
                    0,
                    row_ptr.addr() % agg.function.aggregate_state_info().align
                );
            }
            prev_offset = offset; // To get the next offset relative to this pointer on the next iteration.

            // Update states.
            unsafe {
                agg.function
                    .call_update(selector.inputs, num_rows, group_ptrs)?
            };
        }

        Ok(())
    }

    /// Combines aggregate states, consuming states in `src_ptrs` into
    /// `dest_ptrs`.
    ///
    /// Both sets of pointers should point to the start of each row. Pointers
    /// will be modified, where they point to after this function completes is
    /// not guaranteed.
    ///
    /// # Safety:
    ///
    /// Pointers must point to valid rows according to this layout.
    ///
    /// All pointers **must** point to different rows.
    pub(crate) unsafe fn combine_states(
        &self,
        agg_selection: impl IntoExactSizeIterator<Item = usize>,
        src_ptrs: &mut [*mut u8],
        dest_ptrs: &mut [*mut u8],
    ) -> Result<()> {
        debug_assert_eq!(src_ptrs.len(), dest_ptrs.len());

        let mut prev_offset = 0;

        for (offset, agg) in self.iter_offsets_and_aggregates_selection(agg_selection) {
            let rel_offset = offset - prev_offset;

            // Move both sets of pointers to the right state.
            for row_ptr in src_ptrs.iter_mut() {
                *row_ptr = unsafe { row_ptr.byte_add(rel_offset) };
                debug_assert_eq!(
                    0,
                    row_ptr.addr() % agg.function.aggregate_state_info().align
                );
            }
            for row_ptr in dest_ptrs.iter_mut() {
                *row_ptr = unsafe { row_ptr.byte_add(rel_offset) };
                debug_assert_eq!(
                    0,
                    row_ptr.addr() % agg.function.aggregate_state_info().align
                );
            }
            prev_offset = offset;

            // Combine states.
            unsafe { agg.function.call_combine(src_ptrs, dest_ptrs)? };
        }

        Ok(())
    }

    /// Finalizes states and writes the output the arrays.
    ///
    /// The number of arrays in `outputs` must match the number of aggregates in
    /// this layout.
    ///
    /// Each array must have enough capacity to write len(group_ptrs) number of
    /// states to it.
    ///
    /// # Safety:
    ///
    /// Pointers must point to valid rows according to this layout.
    ///
    /// All pointers **must** point to different rows.
    pub(crate) unsafe fn finalize_states<A>(
        &self,
        group_ptrs: &mut [*mut u8],
        outputs: &mut [A],
    ) -> Result<()>
    where
        A: BorrowMut<Array>,
    {
        unsafe {
            debug_assert_eq!(outputs.len(), self.aggregates.len());

            let mut prev_offset = 0;

            for ((offset, agg), output) in self.iter_offsets_and_aggregates().zip(outputs) {
                let rel_offset = offset - prev_offset;
                for row_ptr in group_ptrs.iter_mut() {
                    *row_ptr = row_ptr.byte_add(rel_offset);
                    debug_assert_eq!(
                        0,
                        row_ptr.addr() % agg.function.aggregate_state_info().align
                    );
                }
                prev_offset = offset;

                // Finalize states.
                agg.function
                    .call_finalize(group_ptrs, output.borrow_mut())?;
            }

            Ok(())
        }
    }
}

/// Iterator that produces aggregate update selectors from the _complete_ set of
/// aggregate inputs.
#[derive(Debug)]
pub struct CompleteInputSelector<'a> {
    layout: &'a AggregateLayout,
    inputs: &'a [Array],
    prev_agg_sel: usize,
    agg_selection: &'a [usize],
}

impl<'a> CompleteInputSelector<'a> {
    pub fn with_selection(
        layout: &'a AggregateLayout,
        agg_selection: &'a [usize],
        inputs: &'a [Array],
    ) -> Self {
        CompleteInputSelector {
            layout,
            inputs,
            prev_agg_sel: 0,
            agg_selection,
        }
    }
}

impl<'a> Iterator for CompleteInputSelector<'a> {
    type Item = AggregateUpdateSelector<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.agg_selection.is_empty() {
            return None;
        }

        let agg_sel = self.agg_selection[0];
        self.agg_selection = &self.agg_selection[1..];

        // Jump to current aggregate, skipping over inputs not part of this
        // aggregate.
        while self.prev_agg_sel != agg_sel {
            let num_inputs = self.layout.aggregates[self.prev_agg_sel].columns.len();
            self.inputs = &self.inputs[num_inputs..];
            self.prev_agg_sel += 1;
        }

        let num_inputs = self.layout.aggregates[agg_sel].columns.len();
        let inputs = &self.inputs[..num_inputs];

        Some(AggregateUpdateSelector {
            aggregate_idx: agg_sel,
            inputs,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.agg_selection.len();
        (rem, Some(rem))
    }
}

impl ExactSizeIterator for CompleteInputSelector<'_> {}

/// Compute the new len to ensure alignment to some value.
const fn align_len(curr_len: usize, alignment: usize) -> usize {
    assert!(alignment != 0, "alignment cannot be zero");
    curr_len.div_ceil(alignment) * alignment
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{self, bind_aggregate_function};
    use crate::functions::aggregate::builtin::minmax::{FUNCTION_SET_MAX, FUNCTION_SET_MIN};

    #[test]
    fn align_len_sanity() {
        struct TestCase {
            curr_len: usize,
            alignment: usize,
            expect: usize,
        }

        let test_cases = [
            TestCase {
                curr_len: 0,
                alignment: 4,
                expect: 0,
            },
            TestCase {
                curr_len: 2,
                alignment: 4,
                expect: 4,
            },
            TestCase {
                curr_len: 13,
                alignment: 4,
                expect: 16,
            },
        ];

        for tc in test_cases {
            let got = align_len(tc.curr_len, tc.alignment);
            assert_eq!(tc.expect, got);
        }
    }

    #[test]
    fn new_no_groups() {
        // MIN_INPUT (col0): Int32
        // MAX_INPUT (col1): Int32
        let min_agg = bind_aggregate_function(
            &FUNCTION_SET_MIN,
            vec![expr::column((0, 0), DataType::Int32).into()],
        )
        .unwrap();
        let max_agg = bind_aggregate_function(
            &FUNCTION_SET_MAX,
            vec![expr::column((0, 1), DataType::Int32).into()],
        )
        .unwrap();

        let aggs = [
            PhysicalAggregateExpression::new(min_agg, [(0, DataType::Int32)]),
            PhysicalAggregateExpression::new(max_agg, [(1, DataType::Int32)]),
        ];

        let layout = AggregateLayout::new([], aggs);

        // Min/max (i32)
        // Align: 4
        // Size:  5 (val + bool)

        assert_eq!(4, layout.base_align);
        assert_eq!(0, layout.aggregate_offsets[0]);
        assert_eq!(8, layout.aggregate_offsets[1]); // Offset aligned to 4
        assert_eq!(16, layout.row_width);
    }
}
