use std::fmt::Debug;

use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{Addressable, MutableScalarStorage, ScalarStorage};
use crate::arrays::executor::{OutBuffer, PutBuffer};
use crate::util::iter::IntoExactSizeIterator;

#[derive(Debug, Clone, Copy)]
pub struct TernaryExecutor;

impl TernaryExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn execute<S1, S2, S3, O, Op>(
        array1: &Array,
        sel1: impl IntoExactSizeIterator<Item = usize>,
        array2: &Array,
        sel2: impl IntoExactSizeIterator<Item = usize>,
        array3: &Array,
        sel3: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer,
        mut op: Op,
    ) -> Result<()>
    where
        S1: ScalarStorage,
        S2: ScalarStorage,
        S3: ScalarStorage,
        O: MutableScalarStorage,
        for<'a> Op: FnMut(
            &S1::StorageType,
            &S2::StorageType,
            &S3::StorageType,
            PutBuffer<O::AddressableMut<'a>>,
        ),
    {
        if array1.should_flatten_for_execution()
            || array2.should_flatten_for_execution()
            || array3.should_flatten_for_execution()
        {
            let flat1 = array1.flatten()?;
            let flat2 = array2.flatten()?;
            let flat3 = array3.flatten()?;

            return Self::execute_flat::<S1, S2, S3, O, _>(
                flat1, sel1, flat2, sel2, flat3, sel3, out, op,
            );
        }

        // TODO: length validation.

        let input1 = S1::get_addressable(&array1.data)?;
        let input2 = S2::get_addressable(&array2.data)?;
        let input3 = S3::get_addressable(&array3.data)?;

        let mut output = O::get_addressable_mut(out.buffer)?;

        let validity1 = &array1.validity;
        let validity2 = &array2.validity;
        let validity3 = &array3.validity;

        if validity1.all_valid() && validity2.all_valid() && validity3.all_valid() {
            for (output_idx, (input1_idx, (input2_idx, input3_idx))) in sel1
                .into_exact_size_iter()
                .zip(sel2.into_exact_size_iter().zip(sel3.into_exact_size_iter()))
                .enumerate()
            {
                let val1 = input1.get(input1_idx).unwrap();
                let val2 = input2.get(input2_idx).unwrap();
                let val3 = input3.get(input3_idx).unwrap();

                op(
                    val1,
                    val2,
                    val3,
                    PutBuffer::new(output_idx, &mut output, out.validity),
                );
            }
        } else {
            for (output_idx, (input1_idx, (input2_idx, input3_idx))) in sel1
                .into_exact_size_iter()
                .zip(sel2.into_exact_size_iter().zip(sel3.into_exact_size_iter()))
                .enumerate()
            {
                if validity1.is_valid(input1_idx)
                    && validity2.is_valid(input2_idx)
                    && validity3.is_valid(input3_idx)
                {
                    let val1 = input1.get(input1_idx).unwrap();
                    let val2 = input2.get(input2_idx).unwrap();
                    let val3 = input3.get(input3_idx).unwrap();

                    op(
                        val1,
                        val2,
                        val3,
                        PutBuffer::new(output_idx, &mut output, out.validity),
                    );
                } else {
                    out.validity.set_invalid(output_idx);
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn execute_flat<'a, S1, S2, S3, O, Op>(
        array1: FlattenedArray<'a>,
        sel1: impl IntoExactSizeIterator<Item = usize>,
        array2: FlattenedArray<'a>,
        sel2: impl IntoExactSizeIterator<Item = usize>,
        array3: FlattenedArray<'a>,
        sel3: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer,
        mut op: Op,
    ) -> Result<()>
    where
        S1: ScalarStorage,
        S2: ScalarStorage,
        S3: ScalarStorage,
        O: MutableScalarStorage,
        for<'b> Op: FnMut(
            &S1::StorageType,
            &S2::StorageType,
            &S3::StorageType,
            PutBuffer<O::AddressableMut<'b>>,
        ),
    {
        // TODO: length validation.

        let input1 = S1::get_addressable(array1.array_buffer)?;
        let input2 = S2::get_addressable(array2.array_buffer)?;
        let input3 = S3::get_addressable(array3.array_buffer)?;

        let mut output = O::get_addressable_mut(out.buffer)?;

        let validity1 = &array1.validity;
        let validity2 = &array2.validity;
        let validity3 = &array3.validity;

        if validity1.all_valid() && validity2.all_valid() && validity3.all_valid() {
            for (output_idx, (input1_idx, (input2_idx, input3_idx))) in sel1
                .into_exact_size_iter()
                .zip(sel2.into_exact_size_iter().zip(sel3.into_exact_size_iter()))
                .enumerate()
            {
                let sel1 = array1.selection.get(input1_idx).unwrap();
                let sel2 = array2.selection.get(input2_idx).unwrap();
                let sel3 = array3.selection.get(input3_idx).unwrap();

                let val1 = input1.get(sel1).unwrap();
                let val2 = input2.get(sel2).unwrap();
                let val3 = input3.get(sel3).unwrap();

                op(
                    val1,
                    val2,
                    val3,
                    PutBuffer::new(output_idx, &mut output, out.validity),
                );
            }
        } else {
            for (output_idx, (input1_idx, (input2_idx, input3_idx))) in sel1
                .into_exact_size_iter()
                .zip(sel2.into_exact_size_iter().zip(sel3.into_exact_size_iter()))
                .enumerate()
            {
                let sel1 = array1.selection.get(input1_idx).unwrap();
                let sel2 = array2.selection.get(input2_idx).unwrap();
                let sel3 = array3.selection.get(input3_idx).unwrap();

                if validity1.is_valid(input1_idx)
                    && validity2.is_valid(input2_idx)
                    && validity3.is_valid(input3_idx)
                {
                    let val1 = input1.get(sel1).unwrap();
                    let val2 = input2.get(sel2).unwrap();
                    let val3 = input3.get(sel3).unwrap();

                    op(
                        val1,
                        val2,
                        val3,
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
    use crate::arrays::array::physical_type::{PhysicalI32, PhysicalUtf8};
    use crate::arrays::datatype::DataType;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn ternary_left_prepend_simple() {
        let strings = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let count = Array::try_from_iter([1, 2, 3]).unwrap();
        let pad = Array::try_from_iter(["<", ".", "!"]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 3).unwrap();

        let mut str_buf = String::new();

        TernaryExecutor::execute::<PhysicalUtf8, PhysicalI32, PhysicalUtf8, PhysicalUtf8, _>(
            &strings,
            0..3,
            &count,
            0..3,
            &pad,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |s, &count, pad, buf| {
                str_buf.clear();
                for _ in 0..count {
                    str_buf.push_str(pad);
                }
                str_buf.push_str(s);

                buf.put(&str_buf);
            },
        )
        .unwrap();

        let expected = Array::try_from_iter(["<a", "..b", "!!!c"]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn ternary_left_prepend_with_invalid() {
        let strings = Array::try_from_iter([Some("a"), None, Some("c")]).unwrap();
        let count = Array::try_from_iter([None, Some(2), Some(3)]).unwrap();
        let pad = Array::try_from_iter(["<", ".", "!"]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 3).unwrap();

        let mut str_buf = String::new();

        TernaryExecutor::execute::<PhysicalUtf8, PhysicalI32, PhysicalUtf8, PhysicalUtf8, _>(
            &strings,
            0..3,
            &count,
            0..3,
            &pad,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |s, &count, pad, buf| {
                str_buf.clear();
                for _ in 0..count {
                    str_buf.push_str(pad);
                }
                str_buf.push_str(s);

                buf.put(&str_buf);
            },
        )
        .unwrap();

        let expected = Array::try_from_iter([None, None, Some("!!!c")]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn ternary_left_prepend_dictionary() {
        let strings = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let count = Array::try_from_iter([1, 2, 3]).unwrap();
        let mut pad = Array::try_from_iter(["<", ".", "!"]).unwrap();
        // '[".", ".", "<"]'
        pad.select(&NopBufferManager, [1, 1, 0]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 3).unwrap();

        let mut str_buf = String::new();

        TernaryExecutor::execute::<PhysicalUtf8, PhysicalI32, PhysicalUtf8, PhysicalUtf8, _>(
            &strings,
            0..3,
            &count,
            0..3,
            &pad,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |s, &count, pad, buf| {
                str_buf.clear();
                for _ in 0..count {
                    str_buf.push_str(pad);
                }
                str_buf.push_str(s);

                buf.put(&str_buf);
            },
        )
        .unwrap();

        let expected = Array::try_from_iter([".a", "..b", "<<<c"]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn ternary_left_prepend_dictionary_with_null() {
        let strings = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let count = Array::try_from_iter([1, 2, 3]).unwrap();
        let mut pad = Array::try_from_iter([Some("<"), None, Some("!")]).unwrap();
        // '[NULL, "!", "<"]'
        pad.select(&NopBufferManager, [1, 2, 0]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 3).unwrap();

        let mut str_buf = String::new();

        TernaryExecutor::execute::<PhysicalUtf8, PhysicalI32, PhysicalUtf8, PhysicalUtf8, _>(
            &strings,
            0..3,
            &count,
            0..3,
            &pad,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |s, &count, pad, buf| {
                str_buf.clear();
                for _ in 0..count {
                    str_buf.push_str(pad);
                }
                str_buf.push_str(s);

                buf.put(&str_buf);
            },
        )
        .unwrap();

        let expected = Array::try_from_iter([None, Some("!!b"), Some("<<<c")]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn ternary_left_prepend_constant() {
        let strings = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let count = Array::try_from_iter([1, 2, 3]).unwrap();
        let pad = Array::new_constant(&NopBufferManager, &"<".into(), 3).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 3).unwrap();

        let mut str_buf = String::new();
        TernaryExecutor::execute::<PhysicalUtf8, PhysicalI32, PhysicalUtf8, PhysicalUtf8, _>(
            &strings,
            0..3,
            &count,
            0..3,
            &pad,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |s, &count, pad, buf| {
                str_buf.clear();
                for _ in 0..count {
                    str_buf.push_str(pad);
                }
                str_buf.push_str(s);

                buf.put(&str_buf);
            },
        )
        .unwrap();

        let expected = Array::try_from_iter(["<a", "<<b", "<<<c"]).unwrap();

        assert_arrays_eq(&expected, &out);
    }
}
