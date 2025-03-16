//! Utilities for asserting array and batch equality.

use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::util::iter::IntoExactSizeIterator;

use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalList,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUtf8,
    ScalarStorage,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::executor::scalar::UnaryExecutor;

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

    let flat1 = array1.flatten().unwrap();
    let flat2 = array2.flatten().unwrap();

    match array1.datatype.physical_type() {
        PhysicalType::Boolean => {
            assert_arrays_eq_sel_inner::<PhysicalBool>(flat1, sel1, flat2, sel2)
        }
        PhysicalType::Int8 => assert_arrays_eq_sel_inner::<PhysicalI8>(flat1, sel1, flat2, sel2),
        PhysicalType::Int16 => assert_arrays_eq_sel_inner::<PhysicalI16>(flat1, sel1, flat2, sel2),
        PhysicalType::Int32 => assert_arrays_eq_sel_inner::<PhysicalI32>(flat1, sel1, flat2, sel2),
        PhysicalType::Int64 => assert_arrays_eq_sel_inner::<PhysicalI64>(flat1, sel1, flat2, sel2),
        PhysicalType::Int128 => {
            assert_arrays_eq_sel_inner::<PhysicalI128>(flat1, sel1, flat2, sel2)
        }
        PhysicalType::UInt8 => assert_arrays_eq_sel_inner::<PhysicalU8>(flat1, sel1, flat2, sel2),
        PhysicalType::UInt16 => assert_arrays_eq_sel_inner::<PhysicalU16>(flat1, sel1, flat2, sel2),
        PhysicalType::UInt32 => assert_arrays_eq_sel_inner::<PhysicalU32>(flat1, sel1, flat2, sel2),
        PhysicalType::UInt64 => assert_arrays_eq_sel_inner::<PhysicalU64>(flat1, sel1, flat2, sel2),
        PhysicalType::UInt128 => {
            assert_arrays_eq_sel_inner::<PhysicalU128>(flat1, sel1, flat2, sel2)
        }
        PhysicalType::Float16 => {
            assert_arrays_eq_sel_inner::<PhysicalF16>(flat1, sel1, flat2, sel2)
        }
        PhysicalType::Float32 => {
            assert_arrays_eq_sel_inner::<PhysicalF32>(flat1, sel1, flat2, sel2)
        }
        PhysicalType::Float64 => {
            assert_arrays_eq_sel_inner::<PhysicalF64>(flat1, sel1, flat2, sel2)
        }
        PhysicalType::Utf8 => assert_arrays_eq_sel_inner::<PhysicalUtf8>(flat1, sel1, flat2, sel2),
        PhysicalType::List => {
            assert_arrays_eq_sel_list_inner(flat1, sel1, flat2, sel2);
        }
        other => unimplemented!("{other:?}"),
    }
}

#[track_caller]
fn assert_arrays_eq_sel_list_inner(
    flat1: FlattenedArray,
    sel1: impl IntoExactSizeIterator<Item = usize>,
    flat2: FlattenedArray,
    sel2: impl IntoExactSizeIterator<Item = usize>,
) {
    unimplemented!()
    // let inner1 = match flat1.array_buffer.get_secondary() {
    //     SecondaryBuffer::List(list) => &list.child,
    //     _ => panic!("Missing child for array 1"),
    // };

    // let inner2 = match flat2.array_buffer.get_secondary() {
    //     SecondaryBuffer::List(list) => &list.child,
    //     _ => panic!("Missing child for array 2"),
    // };

    // let metas1 = PhysicalList::get_addressable(flat1.array_buffer).unwrap();
    // let metas2 = PhysicalList::get_addressable(flat2.array_buffer).unwrap();

    // let sel1 = sel1.into_iter();
    // let sel2 = sel2.into_iter();
    // assert_eq!(sel1.len(), sel2.len());

    // for (row_idx, (idx1, idx2)) in sel1.zip(sel2).enumerate() {
    //     let idx1 = flat1.selection.get(idx1).unwrap();
    //     let idx2 = flat1.selection.get(idx2).unwrap();

    //     assert_eq!(
    //         flat1.validity.is_valid(idx1),
    //         flat2.validity.is_valid(idx2),
    //         "validity mismatch for row {row_idx}"
    //     );

    //     let m1 = metas1.get(idx1).unwrap();
    //     let m2 = metas2.get(idx2).unwrap();

    //     let sel1 = (m1.offset as usize)..((m1.offset + m1.len) as usize);
    //     let sel2 = (m2.offset as usize)..((m2.offset + m2.len) as usize);

    //     assert_arrays_eq_sel(inner1, sel1, inner2, sel2);
    // }
}

#[track_caller]
fn assert_arrays_eq_sel_inner<S>(
    flat1: FlattenedArray,
    sel1: impl IntoExactSizeIterator<Item = usize>,
    flat2: FlattenedArray,
    sel2: impl IntoExactSizeIterator<Item = usize>,
) where
    S: ScalarStorage,
    S::StorageType: ToOwned<Owned: Debug + PartialEq>,
{
    // Maps index to value.
    let mut out = BTreeMap::new();

    UnaryExecutor::for_each_flat::<S, _>(flat1, sel1, |idx, v| {
        out.insert(idx, v.map(|v| v.to_owned()));
    })
    .unwrap();

    // TODO: Bubble up these errors for better line numbers when asserts fail.
    UnaryExecutor::for_each_flat::<S, _>(flat2, sel2, |idx, v| match out.remove(&idx) {
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

    use crate::util::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::buffer::buffer_manager::NopBufferManager;

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
