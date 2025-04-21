//! Utilities for asserting array and batch equality.

use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::arrays::array::Array;
use crate::arrays::array::array_buffer::{ArrayBufferDowncast, ScalarBuffer};
use crate::arrays::array::physical_type::{
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI128,
    PhysicalList,
    PhysicalType,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
    PhysicalUtf8,
    ScalarStorage,
};
use crate::arrays::batch::Batch;
use crate::arrays::compute::copy::copy_rows_array;
use crate::arrays::datatype::DataType;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::field::{ColumnSchema, Field};
use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::expr;
use crate::functions::scalar::builtin::comparison::FUNCTION_SET_IS_NOT_DISTINCT_FROM;
use crate::util::iter::IntoExactSizeIterator;

/// Assert two arrays are logically equal.
///
/// This will assume that the array's capacity is the array's logical length.
#[track_caller]
pub fn assert_arrays_eq(array1: &Array, array2: &Array) {
    assert_eq!(
        array1.logical_len(),
        array2.logical_len(),
        "array capacities differ"
    );

    let sel = 0..array1.logical_len();
    assert_arrays_eq_sel(array1, sel.clone(), array2, sel)
}

/// Asserts that two arrays are logically equal for the first `count` rows.
///
/// This will check valid and invalid values. Assertion error messages will
/// print out Some/None to represent valid/invalid.
#[track_caller]
pub fn assert_arrays_eq_sel(
    array1: &Array,
    sel1: impl IntoExactSizeIterator<Item = usize>,
    array2: &Array,
    sel2: impl IntoExactSizeIterator<Item = usize>,
) {
    assert_eq!(array1.datatype, array2.datatype);

    let sel1 = sel1.into_exact_size_iter();
    let sel2 = sel2.into_exact_size_iter();

    assert_eq!(sel1.len(), sel2.len(), "Array selections differ in lengths");
    let arr_len = sel1.len();

    let mut batch = Batch::new(
        [array1.datatype().clone(), array2.datatype.clone()],
        arr_len,
    )
    .unwrap();
    batch.set_num_rows(arr_len).unwrap();

    // Copy the arrays to a new batch.
    let mapping1 = sel1.enumerate();
    copy_rows_array(array1, mapping1, &mut batch.arrays[0]).unwrap();

    let mapping2 = sel2.enumerate();
    copy_rows_array(array2, mapping2, &mut batch.arrays[1]).unwrap();

    let eq_func = expr::scalar_function(
        &FUNCTION_SET_IS_NOT_DISTINCT_FROM,
        vec![
            expr::column((0, 0), array1.datatype().clone()),
            expr::column((0, 1), array2.datatype().clone()),
        ],
    )
    .unwrap();

    let mut output = Array::new(&DefaultBufferManager, DataType::Boolean, arr_len).unwrap();
    eq_func.function.call_execute(&batch, &mut output).unwrap();

    // Now check if they were all equal.
    assert!(output.validity.all_valid(), "Output not all valid");
    let out_slice = ScalarBuffer::<bool>::downcast_ref(&output.data)
        .unwrap()
        .buffer
        .as_slice();
    assert_eq!(
        out_slice.len(),
        arr_len,
        "Output length doesn't match selection length"
    );
    let not_true_idx = out_slice.iter().position(|&b| !b);
    if let Some(not_true_idx) = not_true_idx {
        // Arrays not equal!
        let table = batch.debug_table();
        panic!("Arrays not equal! Difference found at row index {not_true_idx}\n{table}");
    }

    // Arrays equal!
}

/// Asserts two batches are logically equal.
#[track_caller]
pub fn assert_batches_eq(batch1: &Batch, batch2: &Batch) {
    let arrays1 = batch1.arrays();
    let arrays2 = batch2.arrays();

    assert_eq!(
        arrays1.len(),
        arrays2.len(),
        "batches have different number of arrays"
    );
    assert_eq!(
        batch1.num_rows(),
        batch2.num_rows(),
        "batches have different number of rows"
    );

    for (array1, array2) in arrays1.iter().zip(arrays2) {
        let sel = 0..batch1.num_rows();
        assert_arrays_eq_sel(array1, sel.clone(), array2, sel);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn assert_i32_arrays_eq_simple() {
        let array1 = Array::try_from_iter([4, 5, 6]).unwrap();
        let array2 = Array::try_from_iter([4, 5, 6]).unwrap();

        assert_arrays_eq(&array1, &array2);
    }

    #[test]
    fn assert_i32_arrays_eq_with_dictionary() {
        let array1 = Array::try_from_iter([5, 4, 4]).unwrap();
        let mut array2 = Array::try_from_iter([4, 5]).unwrap();
        array2.select(&DefaultBufferManager, [1, 0, 0]).unwrap();

        assert_arrays_eq(&array1, &array2);
    }

    #[test]
    fn assert_i32_arrays_eq_with_invalid() {
        let mut array1 = Array::try_from_iter([4, 5, 6]).unwrap();
        array1.validity.set_invalid(1);

        let mut array2 = Array::try_from_iter([4, 8, 6]).unwrap();
        array2.validity.set_invalid(1);

        assert_arrays_eq(&array1, &array2);
    }

    #[test]
    fn assert_batches_eq_simple() {
        let batch1 = Batch::from_arrays([
            Array::try_from_iter([4, 5, 6]).unwrap(),
            Array::try_from_iter(["a", "b", "c"]).unwrap(),
        ])
        .unwrap();
        let batch2 = Batch::from_arrays([
            Array::try_from_iter([4, 5, 6]).unwrap(),
            Array::try_from_iter(["a", "b", "c"]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&batch1, &batch2);
    }

    #[test]
    fn assert_batches_eq_logical_row_count() {
        let mut batch1 = Batch::from_arrays([
            Array::try_from_iter([4, 5, 6, 7, 8]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d", "e"]).unwrap(),
        ])
        .unwrap();
        batch1.set_num_rows(3).unwrap();

        let batch2 = Batch::from_arrays([
            Array::try_from_iter([4, 5, 6]).unwrap(),
            Array::try_from_iter(["a", "b", "c"]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&batch1, &batch2);
    }

    #[test]
    #[should_panic]
    fn assert_i32_arrays_eq_not_eq() {
        let array1 = Array::try_from_iter([4, 5, 6]).unwrap();
        let array2 = Array::try_from_iter([4, 5, 7]).unwrap();

        assert_arrays_eq(&array1, &array2);
    }

    #[test]
    #[should_panic]
    fn assert_i32_arrays_different_lengths() {
        let array1 = Array::try_from_iter([4, 5, 6]).unwrap();
        let array2 = Array::try_from_iter([4, 5]).unwrap();

        assert_arrays_eq(&array1, &array2);
    }
}
