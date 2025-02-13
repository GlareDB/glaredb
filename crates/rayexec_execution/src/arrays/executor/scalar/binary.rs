use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{Addressable, MutableScalarStorage, ScalarStorage};
use crate::arrays::array::Array;
use crate::arrays::executor::{OutBuffer, PutBuffer};

#[derive(Debug, Clone, Copy)]
pub struct BinaryExecutor;

impl BinaryExecutor {
    pub fn execute<S1, S2, O, Op, B>(
        array1: &Array<B>,
        sel1: impl IntoExactSizeIterator<Item = usize>,
        array2: &Array<B>,
        sel2: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer<B>,
        mut op: Op,
    ) -> Result<()>
    where
        S1: ScalarStorage,
        S2: ScalarStorage,
        O: MutableScalarStorage,
        for<'a> Op:
            FnMut(&S1::StorageType, &S2::StorageType, PutBuffer<O::AddressableMut<'a, B>, B>),
        B: BufferManager,
    {
        if array1.should_flatten_for_execution() || array2.should_flatten_for_execution() {
            let view1 = FlattenedArray::from_array(array1)?;
            let view2 = FlattenedArray::from_array(array2)?;

            return Self::execute_flat::<S1, S2, _, _, _>(view1, sel1, view2, sel2, out, op);
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

    pub fn execute_flat<'a, S1, S2, O, Op, B>(
        array1: FlattenedArray<'a, B>,
        sel1: impl IntoExactSizeIterator<Item = usize>,
        array2: FlattenedArray<'a, B>,
        sel2: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer<B>,
        mut op: Op,
    ) -> Result<()>
    where
        S1: ScalarStorage,
        S2: ScalarStorage,
        O: MutableScalarStorage,
        for<'b> Op:
            FnMut(&S1::StorageType, &S2::StorageType, PutBuffer<O::AddressableMut<'b, B>, B>),
        B: BufferManager,
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

                if validity1.is_valid(input1_idx) && validity2.is_valid(input2_idx) {
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
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::physical_type::{PhysicalI32, PhysicalUtf8};
    use crate::arrays::array::Array;
    use crate::arrays::datatype::DataType;
    use crate::testutil::arrays::assert_arrays_eq;

    #[test]
    fn binary_simple_add() {
        let left = Array::try_from_iter([1, 2, 3]).unwrap();
        let right = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _, _>(
            &left,
            0..3,
            &right,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&a, &b, buf| buf.put(&(a + b)),
        )
        .unwrap();

        let expected = Array::try_from_iter([5, 7, 9]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn binary_add_with_invalid() {
        let left = Array::try_from_iter([Some(1), None, Some(3)]).unwrap();
        let right = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _, _>(
            &left,
            0..3,
            &right,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&a, &b, buf| buf.put(&(a + b)),
        )
        .unwrap();

        let expected = Array::try_from_iter([Some(5), None, Some(9)]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn binary_add_with_constant() {
        let left = Array::try_from_iter([1, 2, 3]).unwrap();
        let right = Array::new_constant(&NopBufferManager, &4.into(), 3).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _, _>(
            &left,
            0..3,
            &right,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&a, &b, buf| buf.put(&(a + b)),
        )
        .unwrap();

        let expected = Array::try_from_iter([5, 6, 7]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn binary_simple_add_with_constant_selection() {
        let mut left = Array::try_from_iter([2]).unwrap();
        // [2, 2, 2]
        left.select(&NopBufferManager, [0, 0, 0]).unwrap();

        let right = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _, _>(
            &left,
            0..3,
            &right,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&a, &b, buf| buf.put(&(a + b)),
        )
        .unwrap();

        let expected = Array::try_from_iter([6, 7, 8]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn binary_simple_with_selection_invalid() {
        let mut left = Array::try_from_iter([Some(1), None, Some(3)]).unwrap();
        // => [NULL, 3, 1]
        left.select(&NopBufferManager, [1, 2, 0]).unwrap();
        let right = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _, _>(
            &left,
            0..3,
            &right,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&a, &b, buf| buf.put(&(a + b)),
        )
        .unwrap();

        let expected = Array::try_from_iter([None, Some(8), Some(7)]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn binary_string_repeat() {
        let left = Array::try_from_iter([1, 2, 3]).unwrap();
        let right = Array::try_from_iter(["hello", "world", "goodbye!"]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 3).unwrap();

        let mut string_buf = String::new();
        BinaryExecutor::execute::<PhysicalI32, PhysicalUtf8, PhysicalUtf8, _, _>(
            &left,
            0..3,
            &right,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&repeat, s, buf| {
                string_buf.clear();
                for _ in 0..repeat {
                    string_buf.push_str(s);
                }
                buf.put(&string_buf);
            },
        )
        .unwrap();

        let expected =
            Array::try_from_iter(["hello", "worldworld", "goodbye!goodbye!goodbye!"]).unwrap();
        assert_arrays_eq(&expected, &out);
    }
}
