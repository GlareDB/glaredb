use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use super::row_blocks::RowBlocks;
use super::row_layout::RowLayout;
use crate::arrays::array::buffer_manager::{BufferManager, NopBufferManager};
use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{
    Addressable,
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    ScalarStorage,
};
use crate::arrays::array::Array;
use crate::arrays::bitmap::view::BitmapView;
use crate::arrays::view::StringView;
use crate::functions::scalar::builtin::comparison::NullableComparisonOperation;

/// Match state to hold selection/match buffers.
#[derive(Debug)]
pub struct MatchState {
    /// Current set of row indices that have matched.
    row_matches: Vec<usize>,
    /// Selection for rows to try to match.
    row_selection: Vec<usize>,
}

impl MatchState {
    /// Get matched row indices for the most recent call to `find_matches`.
    pub fn get_row_matches(&self) -> &[usize] {
        &self.row_matches
    }
}

/// Matches rows by comparing encoded values with non-encoded values.
#[derive(Debug)]
pub struct PredicateRowMatcher {
    matchers: Vec<Box<dyn Matcher<NopBufferManager>>>,
}

impl PredicateRowMatcher {
    pub fn new(matchers: impl IntoIterator<Item = Box<dyn Matcher<NopBufferManager>>>) -> Self {
        PredicateRowMatcher {
            matchers: matchers.into_iter().collect(),
        }
    }

    /// Initializes a match state.
    pub fn init_match_state(&self) -> MatchState {
        MatchState {
            row_matches: Vec::new(),
            row_selection: Vec::new(),
        }
    }

    /// Finds matches between the lhs and rhs rows.
    ///
    /// Rows for the lhs are provided via row pointers. Rows for the rhs are
    /// provided via (column) arrays.
    ///
    /// The selection indicates which rows from the left _and_ right to compare.
    ///
    /// Each selected row will be compared. If the comparison returns true, then
    /// the index of the row as given by `selection` will be pushed to
    /// `row_matches` in the state.
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
        selection: impl IntoIterator<Item = usize>,
    ) -> Result<usize> {
        state.row_matches.clear();
        state.row_selection.clear();

        // Initialize matches to all rows from the rhs.
        //
        // This gets swapped to the 'selection' buffer in the below loop.
        state.row_matches.extend(selection);

        for ((&lhs_column, rhs_column), matcher) in lhs_columns
            .iter()
            .zip(rhs_columns.iter())
            .zip(&self.matchers)
        {
            // Swap matches/selection so that this next iteration only computes
            // matches on rows that we've previously matched on.
            std::mem::swap(&mut state.row_matches, &mut state.row_selection);

            // Clear matches, each matcher pushes a fresh set of rows matched.
            state.row_matches.clear();

            let rhs_column = rhs_column.flatten()?;

            unsafe {
                matcher.compute_matches(
                    layout,
                    blocks,
                    lhs_rows,
                    lhs_column,
                    rhs_column,
                    &state.row_selection,
                    &mut state.row_matches,
                )?;
            }
        }

        Ok(state.row_matches.len())
    }
}

/// Creates a predicate match for a comparison operation.
pub fn create_predicate_matcher<C>(phys_type: PhysicalType) -> Box<dyn Matcher<NopBufferManager>>
where
    C: NullableComparisonOperation,
{
    match phys_type {
        PhysicalType::UntypedNull => Box::new(ScalarMatcher::<C, PhysicalUntypedNull>::new()),
        PhysicalType::Boolean => Box::new(ScalarMatcher::<C, PhysicalBool>::new()),
        PhysicalType::Int8 => Box::new(ScalarMatcher::<C, PhysicalI8>::new()),
        PhysicalType::Int16 => Box::new(ScalarMatcher::<C, PhysicalI16>::new()),
        PhysicalType::Int32 => Box::new(ScalarMatcher::<C, PhysicalI32>::new()),
        PhysicalType::Int64 => Box::new(ScalarMatcher::<C, PhysicalI64>::new()),
        PhysicalType::Int128 => Box::new(ScalarMatcher::<C, PhysicalI128>::new()),
        PhysicalType::UInt8 => Box::new(ScalarMatcher::<C, PhysicalU8>::new()),
        PhysicalType::UInt16 => Box::new(ScalarMatcher::<C, PhysicalU16>::new()),
        PhysicalType::UInt32 => Box::new(ScalarMatcher::<C, PhysicalU32>::new()),
        PhysicalType::UInt64 => Box::new(ScalarMatcher::<C, PhysicalU64>::new()),
        PhysicalType::UInt128 => Box::new(ScalarMatcher::<C, PhysicalU128>::new()),
        PhysicalType::Float16 => Box::new(ScalarMatcher::<C, PhysicalF16>::new()),
        PhysicalType::Float32 => Box::new(ScalarMatcher::<C, PhysicalF32>::new()),
        PhysicalType::Float64 => Box::new(ScalarMatcher::<C, PhysicalF64>::new()),
        PhysicalType::Interval => Box::new(ScalarMatcher::<C, PhysicalInterval>::new()),
        PhysicalType::Utf8 | PhysicalType::Binary => Box::new(BinaryMatcher::<C>::new()),
        PhysicalType::List => unimplemented!(),
        PhysicalType::Struct => unimplemented!(),
    }
}

pub trait Matcher<B: BufferManager>: Debug + Sync + Send + 'static {
    unsafe fn compute_matches(
        &self,
        layout: &RowLayout,
        _blocks: &RowBlocks<B>,
        lhs_rows: &[*const u8],
        lhs_column: usize,
        rhs_column: FlattenedArray,
        selection: &[usize],
        matches: &mut Vec<usize>,
    ) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
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
        selection: &[usize],
        matches: &mut Vec<usize>,
    ) -> Result<()> {
        let rhs_data = S::get_addressable(rhs_column.array_buffer)?;

        for &row_idx in selection {
            let lhs_row_ptr = lhs_rows[row_idx];

            let validity_buf = layout.validity_buffer(lhs_row_ptr);
            let lhs_valid = BitmapView::new(validity_buf, layout.num_columns()).value(lhs_column);
            let lhs_ptr = lhs_row_ptr.byte_add(layout.offsets[lhs_column]);
            let lhs_ptr = lhs_ptr.cast::<S::StorageType>();
            let lhs_val = lhs_ptr.read_unaligned();

            let rhs_valid = rhs_column.validity.is_valid(row_idx);
            let rhs_sel = rhs_column.selection.get(row_idx).unwrap();
            let rhs_val = rhs_data.get(rhs_sel).unwrap();

            if C::compare_with_valid(lhs_val, *rhs_val, lhs_valid, rhs_valid) {
                matches.push(row_idx);
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct BinaryMatcher<C: NullableComparisonOperation> {
    _c: PhantomData<C>,
}

impl<C> BinaryMatcher<C>
where
    C: NullableComparisonOperation,
{
    const fn new() -> Self {
        BinaryMatcher { _c: PhantomData }
    }
}

impl<C> Matcher<NopBufferManager> for BinaryMatcher<C>
where
    C: NullableComparisonOperation,
{
    unsafe fn compute_matches(
        &self,
        layout: &RowLayout,
        blocks: &RowBlocks<NopBufferManager>,
        lhs_rows: &[*const u8],
        lhs_column: usize,
        rhs_column: FlattenedArray,
        selection: &[usize],
        matches: &mut Vec<usize>,
    ) -> Result<()> {
        let rhs_data = PhysicalBinary::get_addressable(rhs_column.array_buffer)?;

        for &row_idx in selection {
            let lhs_row_ptr = lhs_rows[row_idx];

            let validity_buf = layout.validity_buffer(lhs_row_ptr);
            let lhs_valid = BitmapView::new(validity_buf, layout.num_columns()).value(lhs_column);
            let lhs_ptr = lhs_row_ptr.byte_add(layout.offsets[lhs_column]);
            let lhs_ptr = lhs_ptr.cast::<StringView>();
            let view = lhs_ptr.read_unaligned();

            let lhs_val = if view.is_inline() {
                let inline = view.as_inline();
                &inline.inline[0..inline.len as usize]
            } else {
                let reference = view.as_reference();
                let heap_ptr =
                    blocks.heap_ptr(reference.buffer_idx as usize, reference.offset as usize);
                std::slice::from_raw_parts(heap_ptr, reference.len as usize)
            };

            let rhs_valid = rhs_column.validity.is_valid(row_idx);
            let rhs_sel = rhs_column.selection.get(row_idx).unwrap();
            let rhs_val = rhs_data.get(rhs_sel).unwrap();

            if C::compare_with_valid(lhs_val, rhs_val, lhs_valid, rhs_valid) {
                matches.push(row_idx);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::collection::row::RowCollection;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::generate_batch;
    use crate::functions::scalar::builtin::comparison::{
        EqOperation,
        LtEqOperation,
        NullCoercedComparison,
    };

    #[test]
    fn match_single_i32_eq() {
        let mut collection = RowCollection::new(RowLayout::new([DataType::Int32]), 16);
        let mut append_state = collection.init_append();
        collection
            .append_batch(&mut append_state, &generate_batch!([1, 2, 3, 4]))
            .unwrap();

        let eq =
            create_predicate_matcher::<NullCoercedComparison<EqOperation>>(PhysicalType::Int32);
        let matcher = PredicateRowMatcher::new([eq]);

        let rhs = generate_batch!([0, 2, 5, 4]); // Batch we'll be matching on.

        let mut match_state = matcher.init_match_state();
        let match_count = matcher
            .find_matches(
                &mut match_state,
                collection.layout(),
                collection.blocks(),
                append_state.row_pointers(),
                &[0],
                &rhs.arrays,
                0..4,
            )
            .unwrap();

        assert_eq!(2, match_count);
        assert_eq!(&[1, 3], match_state.get_row_matches());
    }

    #[test]
    fn match_single_nullable_i32_eq() {
        let mut collection = RowCollection::new(RowLayout::new([DataType::Int32]), 16);
        let mut append_state = collection.init_append();
        collection
            .append_batch(
                &mut append_state,
                &generate_batch!([Some(1), None, None, Some(4)]),
            )
            .unwrap();

        let eq =
            create_predicate_matcher::<NullCoercedComparison<EqOperation>>(PhysicalType::Int32);
        let matcher = PredicateRowMatcher::new([eq]);

        let rhs = generate_batch!([Some(0), None, Some(5), Some(4)]);

        let mut match_state = matcher.init_match_state();
        let match_count = matcher
            .find_matches(
                &mut match_state,
                collection.layout(),
                collection.blocks(),
                append_state.row_pointers(),
                &[0],
                &rhs.arrays,
                0..4,
            )
            .unwrap();

        assert_eq!(1, match_count);
        assert_eq!(&[3], match_state.get_row_matches());
    }

    #[test]
    fn match_i32_lteq_and_utf8_eq() {
        let mut collection =
            RowCollection::new(RowLayout::new([DataType::Int32, DataType::Utf8]), 16);
        let mut append_state = collection.init_append();
        collection
            .append_batch(
                &mut append_state,
                &generate_batch!([1, 2, 3, 4], ["cat", "dog", "goose", "moose"]),
            )
            .unwrap();

        let lteq =
            create_predicate_matcher::<NullCoercedComparison<LtEqOperation>>(PhysicalType::Int32);
        let eq = create_predicate_matcher::<NullCoercedComparison<EqOperation>>(PhysicalType::Utf8);
        let matcher = PredicateRowMatcher::new([lteq, eq]);

        let rhs = generate_batch!([1, 2, 5, 7], ["cat", "catdogmouse", "goose", "rooster"]);

        let mut match_state = matcher.init_match_state();
        let match_count = matcher
            .find_matches(
                &mut match_state,
                collection.layout(),
                collection.blocks(),
                append_state.row_pointers(),
                &[0, 1],
                &rhs.arrays,
                0..4,
            )
            .unwrap();

        assert_eq!(2, match_count);
        assert_eq!(&[0, 2], match_state.get_row_matches());
    }
}
