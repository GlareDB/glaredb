use std::borrow::Borrow;
use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use super::row_layout::RowLayout;
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
use crate::arrays::string::StringPtr;
use crate::buffer::buffer_manager::{BufferManager, NopBufferManager};
use crate::expr::comparison_expr::ComparisonOperator;
use crate::functions::scalar::builtin::comparison::{
    EqOperation,
    GtEqOperation,
    GtOperation,
    IsDistinctFromOperator,
    IsNotDistinctFromOperation,
    LtEqOperation,
    LtOperation,
    NotEqOperation,
    NullCoercedComparison,
    NullableComparisonOperation,
};

/// Matches rows by comparing encoded values with non-encoded values.
#[derive(Debug)]
pub struct PredicateRowMatcher {
    matchers: Vec<Box<dyn Matcher<NopBufferManager>>>,
}

impl PredicateRowMatcher {
    /// Create a new predicate row matcher from comparison operators.
    ///
    /// The given iterator provides (type, op) pairs where 'type' is the
    /// physical type for both sides of the comparison.
    pub fn new(matchers: impl IntoIterator<Item = (PhysicalType, ComparisonOperator)>) -> Self {
        let matchers = matchers
            .into_iter()
            .map(|(phys_type, op)| create_predicate_matcher_from_operator(op, phys_type))
            .collect();

        PredicateRowMatcher { matchers }
    }

    /// Finds matches between the lhs and rhs rows, updating `selection` with
    /// the rows that matched.
    ///
    /// Rows for the lhs are provided via row pointers. Rows for the rhs are
    /// provided via (column) arrays.
    ///
    /// `selection` applies to both the lhs and rhs.
    ///
    /// If a row doesn't match, its index will be pushed to `not_matched` Note
    /// that there's no guaranteed order for the pushed indices.
    ///
    /// Returns the number of rows matched.
    pub fn find_matches<A>(
        &self,
        layout: &RowLayout,
        lhs_rows: &[*const u8],
        lhs_columns: &[usize],
        rhs_columns: &[A],
        selection: &mut Vec<usize>,
        not_matched: &mut Vec<usize>,
    ) -> Result<usize>
    where
        A: Borrow<Array>,
    {
        for ((&lhs_column, rhs_column), matcher) in lhs_columns
            .iter()
            .zip(rhs_columns.iter())
            .zip(&self.matchers)
        {
            let rhs_column = rhs_column.borrow().flatten()?;

            unsafe {
                matcher.compute_matches(
                    layout,
                    lhs_rows,
                    lhs_column,
                    rhs_column,
                    selection,
                    not_matched,
                )?;
            }
        }

        Ok(selection.len())
    }
}

fn create_predicate_matcher_from_operator(
    op: ComparisonOperator,
    phys_type: PhysicalType,
) -> Box<dyn Matcher<NopBufferManager>> {
    match op {
        ComparisonOperator::Eq => {
            create_predicate_matcher::<NullCoercedComparison<EqOperation>>(phys_type)
        }
        ComparisonOperator::NotEq => {
            create_predicate_matcher::<NullCoercedComparison<NotEqOperation>>(phys_type)
        }
        ComparisonOperator::Lt => {
            create_predicate_matcher::<NullCoercedComparison<LtOperation>>(phys_type)
        }
        ComparisonOperator::LtEq => {
            create_predicate_matcher::<NullCoercedComparison<LtEqOperation>>(phys_type)
        }
        ComparisonOperator::Gt => {
            create_predicate_matcher::<NullCoercedComparison<GtOperation>>(phys_type)
        }
        ComparisonOperator::GtEq => {
            create_predicate_matcher::<NullCoercedComparison<GtEqOperation>>(phys_type)
        }
        ComparisonOperator::IsDistinctFrom => {
            create_predicate_matcher::<IsDistinctFromOperator>(phys_type)
        }
        ComparisonOperator::IsNotDistinctFrom => {
            create_predicate_matcher::<IsNotDistinctFromOperation>(phys_type)
        }
    }
}

/// Creates a predicate match for a comparison operation.
fn create_predicate_matcher<C>(phys_type: PhysicalType) -> Box<dyn Matcher<NopBufferManager>>
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

trait Matcher<B: BufferManager>: Debug + Sync + Send + 'static {
    unsafe fn compute_matches(
        &self,
        layout: &RowLayout,
        lhs_rows: &[*const u8],
        lhs_column: usize,
        rhs_column: FlattenedArray,
        selection: &mut Vec<usize>,
        not_matches: &mut Vec<usize>,
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
        lhs_rows: &[*const u8],
        lhs_column: usize,
        rhs_column: FlattenedArray,
        selection: &mut Vec<usize>,
        not_matches: &mut Vec<usize>,
    ) -> Result<()> {
        let rhs_data = S::get_addressable(rhs_column.array_buffer)?;

        let mut matches = 0;
        for idx in 0..selection.len() {
            let sel_idx = selection[idx];

            let lhs_row_ptr = lhs_rows[sel_idx];

            let validity_buf = layout.validity_buffer(lhs_row_ptr);
            let lhs_valid = BitmapView::new(validity_buf, layout.num_columns()).value(lhs_column);
            let lhs_ptr = lhs_row_ptr.byte_add(layout.offsets[lhs_column]);
            let lhs_ptr = lhs_ptr.cast::<S::StorageType>();
            let lhs_val = lhs_ptr.read_unaligned();

            let rhs_valid = rhs_column.validity.is_valid(sel_idx);
            let rhs_sel = rhs_column.selection.get(sel_idx).unwrap();
            let rhs_val = rhs_data.get(rhs_sel).unwrap();

            if C::compare_with_valid(lhs_val, *rhs_val, lhs_valid, rhs_valid) {
                selection[matches] = sel_idx;
                matches += 1;
            } else {
                not_matches.push(sel_idx);
            }
        }

        selection.truncate(matches);

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
        lhs_rows: &[*const u8],
        lhs_column: usize,
        rhs_column: FlattenedArray,
        selection: &mut Vec<usize>,
        not_matches: &mut Vec<usize>,
    ) -> Result<()> {
        let rhs_data = PhysicalBinary::get_addressable(rhs_column.array_buffer)?;

        let mut matches = 0;
        for idx in 0..selection.len() {
            let sel_idx = selection[idx];

            let lhs_row_ptr = lhs_rows[sel_idx];

            let validity_buf = layout.validity_buffer(lhs_row_ptr);
            let lhs_valid = BitmapView::new(validity_buf, layout.num_columns()).value(lhs_column);
            let lhs_ptr = lhs_row_ptr.byte_add(layout.offsets[lhs_column]);
            let lhs_ptr = lhs_ptr.cast::<StringPtr>();
            let string_ptr = lhs_ptr.read_unaligned();

            let lhs_val = string_ptr.as_bytes();

            let rhs_valid = rhs_column.validity.is_valid(sel_idx);
            let rhs_sel = rhs_column.selection.get(sel_idx).unwrap();
            let rhs_val = rhs_data.get(rhs_sel).unwrap();

            if C::compare_with_valid(lhs_val, rhs_val, lhs_valid, rhs_valid) {
                selection[matches] = sel_idx;
                matches += 1;
            } else {
                not_matches.push(sel_idx);
            }
        }

        selection.truncate(matches);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::row::row_collection::RowCollection;
    use crate::testutil::arrays::generate_batch;

    /// Helper for finding matches between left and right. Returns (matched,
    /// not_matched).
    ///
    /// This selects all columns and all rows to compare.
    fn find_matches_full(
        matcher: PredicateRowMatcher,
        left: &Batch,
        right: &Batch,
    ) -> (Vec<usize>, Vec<usize>) {
        assert_eq!(left.num_rows, right.num_rows);

        let left_columns: Vec<_> = (0..left.arrays.len()).collect();
        let mut collection = RowCollection::new(
            RowLayout::new(left.arrays.iter().map(|arr| arr.datatype().clone())),
            16,
        );
        let mut append_state = collection.init_append();
        collection.append_batch(&mut append_state, left).unwrap();

        let mut selection: Vec<_> = (0..left.num_rows).collect();
        let mut not_matched = Vec::new();

        let _ = matcher
            .find_matches(
                collection.layout(),
                append_state.row_pointers(),
                &left_columns,
                &right.arrays,
                &mut selection,
                &mut not_matched,
            )
            .unwrap();

        (selection, not_matched)
    }

    #[test]
    fn match_single_i32_eq() {
        let left = generate_batch!([1, 2, 3, 4]);
        let right = generate_batch!([0, 2, 5, 4]);

        let matcher = PredicateRowMatcher::new([(PhysicalType::Int32, ComparisonOperator::Eq)]);
        let (matched, _not_matched) = find_matches_full(matcher, &left, &right);

        assert_eq!(&[1, 3], matched.as_slice());
    }

    #[test]
    fn match_single_nullable_i32_eq() {
        let left = generate_batch!([Some(1), None, None, Some(4)]);
        let right = generate_batch!([Some(0), None, Some(5), Some(4)]);

        let matcher = PredicateRowMatcher::new([(PhysicalType::Int32, ComparisonOperator::Eq)]);
        let (matched, not_matched) = find_matches_full(matcher, &left, &right);

        assert_eq!(&[3], matched.as_slice());
        assert_eq!(&[0, 1, 2], not_matched.as_slice());
    }

    #[test]
    fn match_single_nullable_i32_is_not_distinct_from() {
        let left = generate_batch!([Some(1), None, None, Some(4)]);
        let right = generate_batch!([Some(0), None, Some(5), Some(4)]);

        let matcher = PredicateRowMatcher::new([(
            PhysicalType::Int32,
            ComparisonOperator::IsNotDistinctFrom,
        )]);
        let (matched, not_matched) = find_matches_full(matcher, &left, &right);

        assert_eq!(&[1, 3], matched.as_slice());
        assert_eq!(&[0, 2], not_matched.as_slice());
    }

    #[test]
    fn match_i32_lteq_and_utf8_eq() {
        let left = generate_batch!([1, 2, 3, 4], ["cat", "dog", "goose", "moose"]);
        let right = generate_batch!([1, 2, 5, 7], ["cat", "catdogmouse", "goose", "rooster"]);

        let matcher = PredicateRowMatcher::new([
            (PhysicalType::Int32, ComparisonOperator::LtEq),
            (PhysicalType::Utf8, ComparisonOperator::Eq),
        ]);
        let (matched, not_matched) = find_matches_full(matcher, &left, &right);

        assert_eq!(&[0, 2], matched.as_slice());
        assert_eq!(&[1, 3], not_matched.as_slice());
    }

    #[test]
    fn match_ut8_eq_not_inline() {
        let left = generate_batch!(["cat", "dog", "goosegoosegoosegoosegoose", "moose"]);
        let right = generate_batch!(["cat", "catdogmouse", "goosegoosegoosegoosegoose", "rooster"]);

        let matcher = PredicateRowMatcher::new([(PhysicalType::Utf8, ComparisonOperator::Eq)]);
        let (matched, not_matched) = find_matches_full(matcher, &left, &right);

        assert_eq!(&[0, 2], matched.as_slice());
        assert_eq!(&[1, 3], not_matched.as_slice());
    }

    #[test]
    fn match_many_columns() {
        let left = generate_batch!(
            [1, 2, 3, 4, 5, 6],
            ["a", "b", "c", "d", "e", "f"],
            [1, 2, 3, 4, 5, 6],
            ["a", "b", "c", "d", "e", "f"],
            [1, 2, 3, 4, 5, 6],
            ["a", "b", "c", "d", "e", "f"],
        );
        let right = generate_batch!(
            [1, 2, 3, 4, 5, 6],
            ["b", "b", "c", "d", "e", "f"],
            [1, 3, 3, 4, 5, 6],
            ["a", "b", "d", "d", "e", "f"],
            [1, 2, 3, 5, 5, 6],
            ["a", "b", "c", "d", "f", "f"],
        );

        let matcher = PredicateRowMatcher::new([
            (PhysicalType::Int32, ComparisonOperator::Eq),
            (PhysicalType::Utf8, ComparisonOperator::Eq),
            (PhysicalType::Int32, ComparisonOperator::Eq),
            (PhysicalType::Utf8, ComparisonOperator::Eq),
            (PhysicalType::Int32, ComparisonOperator::Eq),
            (PhysicalType::Utf8, ComparisonOperator::Eq),
        ]);
        let (matched, not_matched) = find_matches_full(matcher, &left, &right);

        assert_eq!(&[5], matched.as_slice());
        assert_eq!(&[0, 1, 2, 3, 4], not_matched.as_slice());
    }
}
