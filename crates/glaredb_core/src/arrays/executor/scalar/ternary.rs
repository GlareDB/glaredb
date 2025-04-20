use std::fmt::Debug;

use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::execution_format::{ExecutionFormat, SelectionFormat};
use crate::arrays::array::physical_type::{Addressable, MutableScalarStorage, ScalarStorage};
use crate::arrays::array::validity::Validity;
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
        let format1 = S1::downcast_execution_format(&array1.data)?;
        let format2 = S2::downcast_execution_format(&array2.data)?;
        let format3 = S3::downcast_execution_format(&array3.data)?;

        match (format1, format2, format3) {
            (ExecutionFormat::Flat(a1), ExecutionFormat::Flat(a2), ExecutionFormat::Flat(a3)) => {
                let input1 = S1::addressable(a1);
                let input2 = S2::addressable(a2);
                let input3 = S3::addressable(a3);

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
            (a1, a2, a3) => {
                let a1 = match a1 {
                    ExecutionFormat::Flat(a1) => SelectionFormat::flat(a1),
                    ExecutionFormat::Selection(a1) => a1,
                };
                let a2 = match a2 {
                    ExecutionFormat::Flat(a2) => SelectionFormat::flat(a2),
                    ExecutionFormat::Selection(a2) => a2,
                };
                let a3 = match a3 {
                    ExecutionFormat::Flat(a3) => SelectionFormat::flat(a3),
                    ExecutionFormat::Selection(a3) => a3,
                };

                Self::execute_selection_format::<S1, S2, S3, _, _>(
                    &array1.validity,
                    a1,
                    sel1,
                    &array2.validity,
                    a2,
                    sel2,
                    &array3.validity,
                    a3,
                    sel3,
                    out,
                    op,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_selection_format<S1, S2, S3, O, Op>(
        validity1: &Validity,
        array1: SelectionFormat<'_, S1::ArrayBuffer>,
        sel1: impl IntoExactSizeIterator<Item = usize>,
        validity2: &Validity,
        array2: SelectionFormat<'_, S2::ArrayBuffer>,
        sel2: impl IntoExactSizeIterator<Item = usize>,
        validity3: &Validity,
        array3: SelectionFormat<'_, S3::ArrayBuffer>,
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
        let input1 = S1::addressable(array1.buffer);
        let input2 = S2::addressable(array2.buffer);
        let input3 = S3::addressable(array3.buffer);

        let mut output = O::get_addressable_mut(out.buffer)?;

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
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn ternary_left_prepend_simple() {
        let strings = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let count = Array::try_from_iter([1, 2, 3]).unwrap();
        let pad = Array::try_from_iter(["<", ".", "!"]).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::Utf8, 3).unwrap();

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

        let mut out = Array::new(&DefaultBufferManager, DataType::Utf8, 3).unwrap();

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
        pad.select(&DefaultBufferManager, [1, 1, 0]).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::Utf8, 3).unwrap();

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
        pad.select(&DefaultBufferManager, [1, 2, 0]).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::Utf8, 3).unwrap();

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
        let pad = Array::new_constant(&DefaultBufferManager, &"<".into(), 3).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::Utf8, 3).unwrap();

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
