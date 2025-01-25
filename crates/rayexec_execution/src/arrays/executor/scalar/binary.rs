use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{Addressable, MutableScalarStorage, ScalarStorage};
use crate::arrays::array::Array;
use crate::arrays::executor::{OutBuffer, PutBuffer};

#[derive(Debug, Clone, Copy)]
pub struct BinaryExecutor;

impl BinaryExecutor {
    pub fn execute<S1, S2, O, Op>(
        array1: &Array,
        sel1: impl IntoExactSizeIterator<Item = usize>,
        array2: &Array,
        sel2: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer,
        mut op: Op,
    ) -> Result<()>
    where
        S1: ScalarStorage,
        S2: ScalarStorage,
        O: MutableScalarStorage,
        for<'a> Op: FnMut(&S1::StorageType, &S2::StorageType, PutBuffer<O::AddressableMut<'a>>),
    {
        if array1.should_flatten_for_execution() || array2.should_flatten_for_execution() {
            let view1 = FlattenedArray::from_array(array1)?;
            let view2 = FlattenedArray::from_array(array2)?;

            return Self::execute_flat::<S1, S2, _, _>(view1, sel1, view2, sel2, out, op);
        }

        // TODO: length validation

        let input1 = S1::get_addressable(&array1.data)?;
        let input2 = S2::get_addressable(&array2.data)?;

        let mut output = O::get_addressable_mut(out.buffer)?;

        let validity1 = &array1.validity;
        let validity2 = &array2.validity;

        if validity1.all_valid() && validity2.all_valid() {
            for (output_idx, (input1_idx, input2_idx)) in sel1
                .into_exact_size_iter()
                .zip(sel2.into_exact_size_iter())
                .enumerate()
            {
                let val1 = input1.get(input1_idx).unwrap();
                let val2 = input2.get(input2_idx).unwrap();

                op(
                    val1,
                    val2,
                    PutBuffer::new(output_idx, &mut output, out.validity),
                );
            }
        } else {
            for (output_idx, (input1_idx, input2_idx)) in sel1
                .into_exact_size_iter()
                .zip(sel2.into_exact_size_iter())
                .enumerate()
            {
                if validity1.is_valid(input1_idx) && validity2.is_valid(input2_idx) {
                    let val1 = input1.get(input1_idx).unwrap();
                    let val2 = input2.get(input2_idx).unwrap();

                    op(
                        val1,
                        val2,
                        PutBuffer::new(output_idx, &mut output, out.validity),
                    );
                } else {
                    out.validity.set_invalid(output_idx);
                }
            }
        }

        Ok(())
    }

    pub fn execute_flat<'a, S1, S2, O, Op>(
        array1: FlattenedArray<'a>,
        sel1: impl IntoExactSizeIterator<Item = usize>,
        array2: FlattenedArray<'a>,
        sel2: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer,
        mut op: Op,
    ) -> Result<()>
    where
        S1: ScalarStorage,
        S2: ScalarStorage,
        O: MutableScalarStorage,
        for<'b> Op: FnMut(&S1::StorageType, &S2::StorageType, PutBuffer<O::AddressableMut<'b>>),
    {
        // TODO: length validation

        let input1 = S1::get_addressable(array1.array_buffer)?;
        let input2 = S2::get_addressable(array2.array_buffer)?;

        let mut output = O::get_addressable_mut(out.buffer)?;

        let validity1 = &array1.validity;
        let validity2 = &array2.validity;

        if validity1.all_valid() && validity2.all_valid() {
            for (output_idx, (input1_idx, input2_idx)) in sel1
                .into_exact_size_iter()
                .zip(sel2.into_exact_size_iter())
                .enumerate()
            {
                let sel1 = array1.selection.get(input1_idx).unwrap();
                let sel2 = array2.selection.get(input2_idx).unwrap();

                let val1 = input1.get(sel1).unwrap();
                let val2 = input2.get(sel2).unwrap();

                op(
                    val1,
                    val2,
                    PutBuffer::new(output_idx, &mut output, out.validity),
                );
            }
        } else {
            for (output_idx, (input1_idx, input2_idx)) in sel1
                .into_exact_size_iter()
                .zip(sel2.into_exact_size_iter())
                .enumerate()
            {
                let sel1 = array1.selection.get(input1_idx).unwrap();
                let sel2 = array2.selection.get(input2_idx).unwrap();

                if validity1.is_valid(sel1) && validity2.is_valid(sel2) {
                    let val1 = input1.get(sel1).unwrap();
                    let val2 = input2.get(sel2).unwrap();

                    op(
                        val1,
                        val2,
                        PutBuffer::new(output_idx, &mut output, out.validity),
                    );
                } else {
                    out.validity.set_invalid(output_idx);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::array_buffer::ArrayBuffer;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::physical_type::{PhysicalI32, PhysicalUtf8};
    use crate::arrays::array::string_view::StringViewHeap;
    use crate::arrays::array::validity::Validity;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_arrays_eq;

    // #[test]
    // fn binary_simple_add() {
    //     let left = Array::try_from_iter([1, 2, 3]).unwrap();
    //     let right = Array::try_from_iter([4, 5, 6]).unwrap();

    //     let mut out =
    //         ArrayBuffer::with_primary_capacity::<PhysicalI32>(&NopBufferManager, 3).unwrap();
    //     let mut validity = Validity::new_all_valid(3);

    //     BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _>(
    //         &left,
    //         0..3,
    //         &right,
    //         0..3,
    //         OutBuffer {
    //             buffer: &mut out,
    //             validity: &mut validity,
    //         },
    //         |&a, &b, buf| buf.put(&(a + b)),
    //     )
    //     .unwrap();
    //     assert!(validity.all_valid());

    //     let out_slice = out.try_as_slice::<PhysicalI32>().unwrap();
    //     assert_eq!(&[5, 7, 9], out_slice);
    // }

    // #[test]
    // fn binary_simple_add_with_selection() {
    //     let mut left = Array::try_from_iter([2]).unwrap();
    //     // [2, 2, 2]
    //     left.select(&NopBufferManager, [0, 0, 0]).unwrap();

    //     let right = Array::try_from_iter([4, 5, 6]).unwrap();

    //     let mut out =
    //         ArrayBuffer::with_primary_capacity::<PhysicalI32>(&NopBufferManager, 3).unwrap();
    //     let mut validity = Validity::new_all_valid(3);

    //     BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _>(
    //         &left,
    //         0..3,
    //         &right,
    //         0..3,
    //         OutBuffer {
    //             buffer: &mut out,
    //             validity: &mut validity,
    //         },
    //         |&a, &b, buf| buf.put(&(a + b)),
    //     )
    //     .unwrap();
    //     assert!(validity.all_valid());

    //     let out_slice = out.try_as_slice::<PhysicalI32>().unwrap();
    //     assert_eq!(&[6, 7, 8], out_slice);
    // }

    // #[test]
    // fn binary_string_repeat() {
    //     let left = Array::try_from_iter([1, 2, 3]).unwrap();
    //     let right = Array::try_from_iter(["hello", "world", "goodbye!"]).unwrap();

    //     let mut out =
    //         ArrayBuffer::with_primary_capacity::<PhysicalUtf8>(&NopBufferManager, 3).unwrap();
    //     out.put_secondary_buffer(SecondaryBuffer::StringViewHeap(StringViewHeap::new()));
    //     let mut validity = Validity::new_all_valid(3);

    //     let mut string_buf = String::new();
    //     BinaryExecutor::execute::<PhysicalI32, PhysicalUtf8, PhysicalUtf8, _>(
    //         &left,
    //         0..3,
    //         &right,
    //         0..3,
    //         OutBuffer {
    //             buffer: &mut out,
    //             validity: &mut validity,
    //         },
    //         |&repeat, s, buf| {
    //             string_buf.clear();
    //             for _ in 0..repeat {
    //                 string_buf.push_str(s);
    //             }
    //             buf.put(&string_buf);
    //         },
    //     )
    //     .unwrap();
    //     assert!(validity.all_valid());

    //     let out = out.try_as_string_view_addressable().unwrap();
    //     assert_eq!("hello", out.get(0).unwrap());
    //     assert_eq!("worldworld", out.get(1).unwrap());
    //     assert_eq!("goodbye!goodbye!goodbye!", out.get(2).unwrap());
    // }

    // #[test]
    // fn binary_add_with_invalid() {
    //     let left = Array::try_from_iter([Some(1), None, Some(3)]).unwrap();
    //     let right = Array::try_from_iter([4, 5, 6]).unwrap();

    //     let mut out =
    //         ArrayBuffer::with_primary_capacity::<PhysicalI32>(&NopBufferManager, 3).unwrap();
    //     let mut validity = Validity::new_all_valid(3);

    //     BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _>(
    //         &left,
    //         0..3,
    //         &right,
    //         0..3,
    //         OutBuffer {
    //             buffer: &mut out,
    //             validity: &mut validity,
    //         },
    //         |&a, &b, buf| buf.put(&(a + b)),
    //     )
    //     .unwrap();

    //     let out_slice = out.try_as_slice::<PhysicalI32>().unwrap();

    //     assert!(validity.is_valid(0));
    //     assert_eq!(5, out_slice[0]);

    //     assert!(!validity.is_valid(1));

    //     assert!(validity.is_valid(2));
    //     assert_eq!(9, out_slice[2]);
    // }

    // #[test]
    // fn binary_add_with_constant() {
    //     let left = Array::try_from_iter([1, 2, 3]).unwrap();
    //     let right = Array::try_new_constant(&NopBufferManager, &4.into(), 3).unwrap();

    //     let mut out = Array::try_new(&NopBufferManager, DataType::Int32, 3).unwrap();

    //     BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _>(
    //         &left,
    //         0..3,
    //         &right,
    //         0..3,
    //         OutBuffer::from_array(&mut out).unwrap(),
    //         |&a, &b, buf| buf.put(&(a + b)),
    //     )
    //     .unwrap();

    //     let expected = Array::try_from_iter([5, 6, 7]).unwrap();
    //     assert_arrays_eq(&expected, &out);
    // }
}
