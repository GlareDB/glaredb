use std::collections::BTreeMap;
use std::fmt::Debug;

use iterutil::exact_size::IntoExactSizeIterator;

use super::array::Array;
use super::batch::Batch;
use super::buffer::{Int32BufferBuilder, StringBufferBuilder};
use super::buffer_manager::NopBufferManager;
use super::datatype::DataType;
use crate::arrays::buffer::physical_type::{PhysicalBoolean, PhysicalI32, PhysicalStorage, PhysicalType, PhysicalUtf8};
use crate::arrays::buffer::ArrayBuffer;
use crate::arrays::executor::scalar::binary::BinaryExecutor;
use crate::arrays::executor::scalar::unary::UnaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::arrays::flat_array::FlatArrayView;
use crate::arrays::validity::Validity;

pub fn new_i32_array(vals: impl IntoExactSizeIterator<Item = i32>) -> Array {
    Array::new_with_buffer(DataType::Int32, Int32BufferBuilder::from_iter(vals).unwrap())
}

pub fn new_string_array<'a>(vals: impl IntoExactSizeIterator<Item = &'a str>) -> Array {
    Array::new_with_buffer(DataType::Utf8, StringBufferBuilder::from_iter(vals).unwrap())
}

pub fn new_batch_from_arrays(arrays: impl IntoIterator<Item = Array>) -> Batch {
    Batch::from_arrays(arrays, true).unwrap()
}

/// Assert two arrays are logically equal.
///
/// This will assume that the array's capacity is the array's logical length.
pub fn assert_arrays_eq(array1: &Array, array2: &Array) {
    assert_eq!(array1.capacity(), array2.capacity(), "array capacities differ");
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
        PhysicalType::Int32 => assert_eq_inner::<PhysicalI32>(flat1, flat2, count),
        PhysicalType::Utf8 => assert_eq_inner::<PhysicalUtf8>(flat1, flat2, count),
        other => unimplemented!("{other:?}"),
    }
}

/// Asserts two batches are logically equal.
pub fn assert_batches_eq(batch1: &Batch, batch2: &Batch) {
    let arrays1 = batch1.arrays();
    let arrays2 = batch2.arrays();

    assert_eq!(arrays1.len(), arrays2.len(), "batches have different number of arrays");
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
    use super::*;

    #[test]
    fn assert_i32_arrays_eq_simple() {
        let array1 = new_i32_array([4, 5, 6]);
        let array2 = new_i32_array([4, 5, 6]);

        assert_arrays_eq(&array1, &array2);
    }

    #[test]
    fn assert_i32_arrays_eq_with_dictionary() {
        let array1 = new_i32_array([5, 4, 4]);
        let mut array2 = new_i32_array([4, 5]);
        array2.select(&NopBufferManager, [1, 0, 0]).unwrap();

        assert_arrays_eq(&array1, &array2);
    }

    #[test]
    fn assert_i32_arrays_eq_with_invalid() {
        let mut array1 = new_i32_array([4, 5, 6]);
        array1.validity.set_invalid(1);

        let mut array2 = new_i32_array([4, 8, 6]);
        array2.validity.set_invalid(1);

        assert_arrays_eq(&array1, &array2);
    }

    #[test]
    fn assert_batches_eq_simple() {
        let batch1 = new_batch_from_arrays([new_i32_array([4, 5, 6]), new_string_array(["a", "b", "c"])]);
        let batch2 = new_batch_from_arrays([new_i32_array([4, 5, 6]), new_string_array(["a", "b", "c"])]);

        assert_batches_eq(&batch1, &batch2);
    }

    #[test]
    fn assert_batches_eq_logical_row_count() {
        let mut batch1 = new_batch_from_arrays([
            new_i32_array([4, 5, 6, 7, 8]),
            new_string_array(["a", "b", "c", "d", "e"]),
        ]);
        batch1.set_num_rows(3).unwrap();

        let batch2 = new_batch_from_arrays([new_i32_array([4, 5, 6]), new_string_array(["a", "b", "c"])]);

        assert_batches_eq(&batch1, &batch2);
    }

    #[test]
    #[should_panic]
    fn assert_i32_arrays_eq_not_eq() {
        let array1 = new_i32_array([4, 5, 6]);
        let array2 = new_i32_array([4, 5, 7]);

        assert_arrays_eq(&array1, &array2);
    }

    #[test]
    #[should_panic]
    fn assert_i32_arrays_different_lengths() {
        let array1 = new_i32_array([4, 5, 6]);
        let array2 = new_i32_array([4, 5]);

        assert_arrays_eq(&array1, &array2);
    }
}
