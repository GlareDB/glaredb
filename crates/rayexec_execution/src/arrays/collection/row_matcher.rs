use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use super::row_blocks::RowBlocks;
use super::row_layout::RowLayout;
use crate::arrays::array::buffer_manager::{BufferManager, NopBufferManager};
use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{Addressable, PhysicalBool, PhysicalType, ScalarStorage};
use crate::arrays::array::Array;
use crate::arrays::bitmap::view::BitmapView;
use crate::functions::scalar::builtin::comparison::NullableComparisonOperation;

/// Match state to hold selection/match buffers.
#[derive(Debug)]
pub struct MatchState {
    /// Current set of rhs indices that have matched.
    rhs_matches: Vec<usize>,
    /// Selection for rows to try to match on the rhs.
    rhs_selection: Vec<usize>,
}

impl MatchState {
    /// Get rhs indices for the most recent call to `find_matches`.
    pub fn get_rhs_matches(&self) -> &[usize] {
        &self.rhs_matches
    }
}

/// Matches rows by comparing encoded values with non-encoded values.
#[derive(Debug)]
pub struct PredicateRowMatcher {
    matchers: Vec<Box<dyn Matcher<NopBufferManager>>>,
}

impl PredicateRowMatcher {
    pub fn init_match_state(&self) -> MatchState {
        MatchState {
            rhs_matches: Vec::new(),
            rhs_selection: Vec::new(),
        }
    }

    /// Finds matches between the lhs and rhs rows.
    ///
    /// Rows on the lhs are implicitly selected via what row pointers we
    /// received. The rhs is selected via `rhs_selection`. This selection must
    /// match the number of row pointers we receive for the lhs.
    ///
    /// Each row will be compared. If the comparison returns true, then the
    /// index of the row as given by `rhs_selection` will be pushed to
    /// `rhs_matches` in the state.
    ///
    /// Returns the number of rows matched.
    pub fn find_matches(
        &self,
        state: &mut MatchState,
        layout: &RowLayout,
        blocks: &RowBlocks<NopBufferManager>,
        lhs_rows: &[*const u8],
        lhs_columns: &[usize],
        rhs_columns: &[Array],
        rhs_selection: &[usize],
    ) -> Result<usize> {
        debug_assert_eq!(lhs_rows.len(), rhs_selection.len());

        state.rhs_matches.clear();
        state.rhs_selection.clear();

        // Initialize matches to all rows from the rhs.
        //
        // This gets swapped to the 'selection' buffer in the below loop.
        state.rhs_matches.extend_from_slice(rhs_selection);

        for ((&lhs_column, rhs_column), matcher) in lhs_columns
            .iter()
            .zip(rhs_columns.iter())
            .zip(&self.matchers)
        {
            // Swap matches/selection so that this next iteration only computes
            // matches on rows that we've previously matched on.
            std::mem::swap(&mut state.rhs_matches, &mut state.rhs_selection);

            // Clear matches, each matcher pushes a fresh set of rows matched.
            state.rhs_matches.clear();

            let rhs_column = rhs_column.flatten()?;

            unsafe {
                matcher.compute_matches(
                    layout,
                    blocks,
                    lhs_rows,
                    lhs_column,
                    rhs_column,
                    rhs_selection,
                    &mut state.rhs_matches,
                )?;
            }
        }

        Ok(state.rhs_matches.len())
    }
}

fn create_predicate_matcher<C>(
    phys_type: PhysicalType,
) -> Result<Box<dyn Matcher<NopBufferManager>>>
where
    C: NullableComparisonOperation,
{
    Ok(match phys_type {
        PhysicalType::Boolean => Box::new(ScalarMatcher::<C, PhysicalBool>::new()),
        _ => unimplemented!(),
    })
}

trait Matcher<B: BufferManager>: Debug + Sync + Send + 'static {
    unsafe fn compute_matches(
        &self,
        layout: &RowLayout,
        _blocks: &RowBlocks<B>,
        lhs_rows: &[*const u8],
        lhs_column: usize,
        rhs_column: FlattenedArray,
        rhs_selection: &[usize],
        rhs_matches: &mut Vec<usize>,
    ) -> Result<()>;
}

#[derive(Debug)]
struct ScalarMatcher<C: NullableComparisonOperation, S: ScalarStorage> {
    _c: PhantomData<C>,
    _s: PhantomData<S>,
}

impl<C, S> ScalarMatcher<C, S>
where
    C: NullableComparisonOperation,
    S: ScalarStorage,
{
    const fn new() -> Self {
        ScalarMatcher {
            _c: PhantomData,
            _s: PhantomData,
        }
    }
}

impl<C, S> Matcher<NopBufferManager> for ScalarMatcher<C, S>
where
    C: NullableComparisonOperation,
    S: ScalarStorage,
    S::StorageType: PartialEq + PartialOrd + Copy + Sized,
{
    unsafe fn compute_matches(
        &self,
        layout: &RowLayout,
        _blocks: &RowBlocks<NopBufferManager>,
        lhs_rows: &[*const u8],
        lhs_column: usize,
        rhs_column: FlattenedArray,
        rhs_selection: &[usize],
        rhs_matches: &mut Vec<usize>,
    ) -> Result<()> {
        let rhs_data = S::get_addressable(rhs_column.array_buffer)?;

        for (&lhs_row_ptr, &rhs_row_idx) in lhs_rows.iter().zip(rhs_selection) {
            let validity_buf = layout.validity_buffer(lhs_row_ptr);
            let lhs_valid = BitmapView::new(validity_buf, layout.num_columns()).value(lhs_column);
            let lhs_ptr = lhs_row_ptr.byte_add(layout.offsets[lhs_column]);
            let lhs_ptr = lhs_ptr.cast::<S::StorageType>();
            let lhs_val = lhs_ptr.read_unaligned();

            let rhs_valid = rhs_column.validity.is_valid(rhs_row_idx);
            let rhs_sel = rhs_column.selection.get(rhs_row_idx).unwrap();
            let rhs_val = rhs_data.get(rhs_sel).unwrap();

            if C::compare_with_valid(lhs_val, *rhs_val, lhs_valid, rhs_valid) {
                rhs_matches.push(rhs_row_idx);
            }
        }

        Ok(())
    }
}
