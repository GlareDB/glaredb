use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::execution_format::{ExecutionFormat, SelectionFormat};
use crate::arrays::array::physical_type::{Addressable, MutableScalarStorage, ScalarStorage};
use crate::arrays::array::validity::Validity;
use crate::arrays::executor::{OutBuffer, PutBuffer};
use crate::util::iter::IntoExactSizeIterator;

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
        let format1 = S1::downcast_execution_format(&array1.data)?;
        let format2 = S2::downcast_execution_format(&array2.data)?;

        match (format1, format2) {
            (ExecutionFormat::Flat(a1), ExecutionFormat::Flat(a2)) => {
                let input1 = S1::addressable(a1);
                let input2 = S2::addressable(a2);

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
            (a1, a2) => {
                let a1 = match a1 {
                    ExecutionFormat::Flat(a1) => SelectionFormat::flat(a1),
                    ExecutionFormat::Selection(a1) => a1,
                };
                let a2 = match a2 {
                    ExecutionFormat::Flat(a2) => SelectionFormat::flat(a2),
                    ExecutionFormat::Selection(a2) => a2,
                };

                Self::execute_with_selection::<S1, S2, O, _>(
                    &array1.validity,
                    a1,
                    sel1,
                    &array2.validity,
                    a2,
                    sel2,
                    out,
                    op,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_with_selection<S1, S2, O, Op>(
        validity1: &Validity,
        array1: SelectionFormat<'_, S1::ArrayBuffer>,
        sel1: impl IntoExactSizeIterator<Item = usize>,
        validity2: &Validity,
        array2: SelectionFormat<'_, S2::ArrayBuffer>,
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
        let input1 = S1::addressable(array1.buffer);
        let input2 = S2::addressable(array2.buffer);

        let mut output = O::get_addressable_mut(out.buffer)?;

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
                if validity1.is_valid(input1_idx) && validity2.is_valid(input2_idx) {
                    let sel1 = array1.selection.get(input1_idx).unwrap();
                    let sel2 = array2.selection.get(input2_idx).unwrap();

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
    use super::*;
    use crate::arrays::array::Array;
    use crate::arrays::array::physical_type::{PhysicalI32, PhysicalUtf8};
    use crate::arrays::datatype::DataType;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn binary_simple_add() {
        let left = Array::try_from_iter([1, 2, 3]).unwrap();
        let right = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::Int32, 3).unwrap();

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _>(
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

        let mut out = Array::new(&DefaultBufferManager, DataType::Int32, 3).unwrap();

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _>(
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
        let right = Array::new_constant(&DefaultBufferManager, &4.into(), 3).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::Int32, 3).unwrap();

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _>(
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
        left.select(&DefaultBufferManager, [0, 0, 0]).unwrap();

        let right = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::Int32, 3).unwrap();

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _>(
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
        left.select(&DefaultBufferManager, [1, 2, 0]).unwrap();
        let right = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::Int32, 3).unwrap();

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _>(
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

        let mut out = Array::new(&DefaultBufferManager, DataType::Utf8, 3).unwrap();

        let mut string_buf = String::new();
        BinaryExecutor::execute::<PhysicalI32, PhysicalUtf8, PhysicalUtf8, _>(
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
