//! Test utilities.
//!
//! Note this this isn't behind a `#[cfg(test)]` flag since this should be
//! usable outside of this crate.
//!
//! Should not be used outside of tests.

use std::collections::BTreeMap;
use std::fmt::Debug;

use super::array::exp::Array;
use super::batch_exp::Batch;
use crate::arrays::array::flat::FlatArrayView;
use crate::arrays::buffer::physical_type::{
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalStorage,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUtf8,
};
use crate::arrays::executor_exp::scalar::unary::UnaryExecutor;

/// Assert two arrays are logically equal.
///
/// This will assume that the array's capacity is the array's logical length.
pub fn assert_arrays_eq(array1: &Array, array2: &Array) {
    assert_eq!(
        array1.capacity(),
        array2.capacity(),
        "array capacities differ"
    );
    assert_arrays_eq_count(array1, array2, array1.capacity())
}

/// Asserts that two arrays are logically equal for the first `count` rows.
///
/// This will check valid and invalid values. Assertion error messages will
/// print out Some/None to represent valid/invalid.
pub fn assert_arrays_eq_count(array1: &Array, array2: &Array, count: usize) {
    assert_eq!(array1.datatype, array2.datatype);

    let flat1 = array1.flat_view().unwrap();
    let flat2 = array2.flat_view().unwrap();

    fn assert_eq_inner<S>(flat1: FlatArrayView, flat2: FlatArrayView, count: usize)
    where
        S: PhysicalStorage,
        S::StorageType: ToOwned<Owned: Debug + PartialEq>,
    {
        let mut out = BTreeMap::new();
        let sel = 0..count;

        UnaryExecutor::for_each_flat::<S, _>(flat1, sel.clone(), |idx, v| {
            out.insert(idx, v.map(|v| v.to_owned()));
        })
        .unwrap();

        UnaryExecutor::for_each_flat::<S, _>(flat2, sel, |idx, v| match out.remove(&idx) {
            Some(existing) => {
                let v = v.map(|v| v.to_owned());
                assert_eq!(existing, v, "values differ at index {idx}");
            }
            None => panic!("missing value for index in array 1 {idx}"),
        })
        .unwrap();

        if !out.is_empty() {
            panic!("extra entries in array 1: {:?}", out);
        }
    }

    match array1.datatype.physical_type() {
        PhysicalType::Boolean => assert_eq_inner::<PhysicalBool>(flat1, flat2, count),
        PhysicalType::Int8 => assert_eq_inner::<PhysicalI8>(flat1, flat2, count),
        PhysicalType::Int16 => assert_eq_inner::<PhysicalI16>(flat1, flat2, count),
        PhysicalType::Int32 => assert_eq_inner::<PhysicalI32>(flat1, flat2, count),
        PhysicalType::Int64 => assert_eq_inner::<PhysicalI64>(flat1, flat2, count),
        PhysicalType::Int128 => assert_eq_inner::<PhysicalI128>(flat1, flat2, count),
        PhysicalType::UInt8 => assert_eq_inner::<PhysicalU8>(flat1, flat2, count),
        PhysicalType::UInt16 => assert_eq_inner::<PhysicalU16>(flat1, flat2, count),
        PhysicalType::UInt32 => assert_eq_inner::<PhysicalU32>(flat1, flat2, count),
        PhysicalType::UInt64 => assert_eq_inner::<PhysicalU64>(flat1, flat2, count),
        PhysicalType::UInt128 => assert_eq_inner::<PhysicalU128>(flat1, flat2, count),
        PhysicalType::Float16 => assert_eq_inner::<PhysicalF16>(flat1, flat2, count),
        PhysicalType::Float32 => assert_eq_inner::<PhysicalF32>(flat1, flat2, count),
        PhysicalType::Float64 => assert_eq_inner::<PhysicalF64>(flat1, flat2, count),
        PhysicalType::Utf8 => assert_eq_inner::<PhysicalUtf8>(flat1, flat2, count),
        other => unimplemented!("{other:?}"),
    }
}

/// Asserts two batches are logically equal.
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
        assert_arrays_eq_count(array1, array2, batch1.num_rows());
    }
}

#[cfg(test)]
mod tests {
    use iterutil::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::buffer::buffer_manager::NopBufferManager;

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
        array2.select(&NopBufferManager, [1, 0, 0]).unwrap();

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
        let batch1 = Batch::from_arrays(
            [
                Array::try_from_iter([4, 5, 6]).unwrap(),
                Array::try_from_iter(["a", "b", "c"]).unwrap(),
            ],
            true,
        )
        .unwrap();
        let batch2 = Batch::from_arrays(
            [
                Array::try_from_iter([4, 5, 6]).unwrap(),
                Array::try_from_iter(["a", "b", "c"]).unwrap(),
            ],
            true,
        )
        .unwrap();

        assert_batches_eq(&batch1, &batch2);
    }

    #[test]
    fn assert_batches_eq_logical_row_count() {
        let mut batch1 = Batch::from_arrays(
            [
                Array::try_from_iter([4, 5, 6, 7, 8]).unwrap(),
                Array::try_from_iter(["a", "b", "c", "d", "e"]).unwrap(),
            ],
            false,
        )
        .unwrap();
        batch1.set_num_rows(3).unwrap();

        let batch2 = Batch::from_arrays(
            [
                Array::try_from_iter([4, 5, 6]).unwrap(),
                Array::try_from_iter(["a", "b", "c"]).unwrap(),
            ],
            true,
        )
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
