use std::fmt::Debug;

use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use super::check_validity;
use crate::arrays::array::flat::FlatArrayView;
use crate::arrays::array::physical_type::{Addressable, MutablePhysicalStorage, PhysicalStorage};
use crate::arrays::array::Array;
use crate::arrays::bitmap::Bitmap;
use crate::arrays::executor::builder::{ArrayBuilder, ArrayDataBuffer, OutputBuffer};
use crate::arrays::executor::scalar::validate_logical_len;
use crate::arrays::executor::{OutBuffer, PutBuffer};
use crate::arrays::selection;
use crate::arrays::storage::AddressableStorage;

#[derive(Debug, Clone, Copy)]
pub struct TernaryExecutor;

impl TernaryExecutor {
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
        S1: PhysicalStorage,
        S2: PhysicalStorage,
        S3: PhysicalStorage,
        O: MutablePhysicalStorage,
        for<'a> Op: FnMut(
            &S1::StorageType,
            &S2::StorageType,
            &S3::StorageType,
            PutBuffer<O::AddressableMut<'a>>,
        ),
    {
        if array1.is_dictionary() || array2.is_dictionary() || array3.is_dictionary() {
            let flat1 = array1.flat_view()?;
            let flat2 = array2.flat_view()?;
            let flat3 = array3.flat_view()?;

            return Self::execute_flat::<S1, S2, S3, O, _>(
                flat1, sel1, flat2, sel2, flat3, sel3, out, op,
            );
        }

        // TODO: length validation.

        let input1 = S1::get_addressable(&array1.next().data)?;
        let input2 = S2::get_addressable(&array2.next().data)?;
        let input3 = S3::get_addressable(&array3.next().data)?;

        let mut output = O::get_addressable_mut(out.buffer)?;

        let validity1 = &array1.next().validity;
        let validity2 = &array2.next().validity;
        let validity3 = &array3.next().validity;

        if validity1.all_valid() && validity2.all_valid() && validity3.all_valid() {
            for (output_idx, (input1_idx, (input2_idx, input3_idx))) in sel1
                .into_iter()
                .zip(sel2.into_iter().zip(sel3.into_iter()))
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
                .into_iter()
                .zip(sel2.into_iter().zip(sel3.into_iter()))
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

    pub fn execute_flat<'a, S1, S2, S3, O, Op>(
        array1: FlatArrayView<'a>,
        sel1: impl IntoExactSizeIterator<Item = usize>,
        array2: FlatArrayView<'a>,
        sel2: impl IntoExactSizeIterator<Item = usize>,
        array3: FlatArrayView<'a>,
        sel3: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer,
        mut op: Op,
    ) -> Result<()>
    where
        S1: PhysicalStorage,
        S2: PhysicalStorage,
        S3: PhysicalStorage,
        O: MutablePhysicalStorage,
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
                .into_iter()
                .zip(sel2.into_iter().zip(sel3.into_iter()))
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
                .into_iter()
                .zip(sel2.into_iter().zip(sel3.into_iter()))
                .enumerate()
            {
                let sel1 = array1.selection.get(input1_idx).unwrap();
                let sel2 = array2.selection.get(input2_idx).unwrap();
                let sel3 = array3.selection.get(input3_idx).unwrap();

                if validity1.is_valid(sel1) && validity2.is_valid(sel2) && validity3.is_valid(sel3)
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

    pub fn execute2<'a, S1, S2, S3, B, Op>(
        array1: &'a Array,
        array2: &'a Array,
        array3: &'a Array,
        builder: ArrayBuilder<B>,
        mut op: Op,
    ) -> Result<Array>
    where
        Op: FnMut(S1::Type<'a>, S2::Type<'a>, S3::Type<'a>, &mut OutputBuffer<B>),
        S1: PhysicalStorage,
        S2: PhysicalStorage,
        S3: PhysicalStorage,
        B: ArrayDataBuffer,
    {
        let len = validate_logical_len(&builder.buffer, array1)?;
        let _ = validate_logical_len(&builder.buffer, array2)?;
        let _ = validate_logical_len(&builder.buffer, array3)?;

        let selection1 = array1.selection_vector();
        let selection2 = array2.selection_vector();
        let selection3 = array3.selection_vector();

        let validity1 = array1.validity();
        let validity2 = array2.validity();
        let validity3 = array3.validity();

        let mut out_validity = None;

        let mut output_buffer = OutputBuffer {
            idx: 0,
            buffer: builder.buffer,
        };

        if validity1.is_some() || validity2.is_some() || validity3.is_some() {
            let values1 = S1::get_storage(&array1.data2)?;
            let values2 = S2::get_storage(&array2.data2)?;
            let values3 = S3::get_storage(&array3.data2)?;

            let mut out_validity_builder = Bitmap::new_with_all_true(len);

            for idx in 0..len {
                let sel1 = selection::get(selection1, idx);
                let sel2 = selection::get(selection2, idx);
                let sel3 = selection::get(selection3, idx);

                if check_validity(sel1, validity1)
                    && check_validity(sel2, validity2)
                    && check_validity(sel3, validity3)
                {
                    let val1 = unsafe { values1.get_unchecked(sel1) };
                    let val2 = unsafe { values2.get_unchecked(sel2) };
                    let val3 = unsafe { values3.get_unchecked(sel3) };

                    output_buffer.idx = idx;
                    op(val1, val2, val3, &mut output_buffer);
                } else {
                    out_validity_builder.set_unchecked(idx, false);
                }
            }

            out_validity = Some(out_validity_builder.into())
        } else {
            let values1 = S1::get_storage(&array1.data2)?;
            let values2 = S2::get_storage(&array2.data2)?;
            let values3 = S3::get_storage(&array3.data2)?;

            for idx in 0..len {
                let sel1 = selection::get(selection1, idx);
                let sel2 = selection::get(selection2, idx);
                let sel3 = selection::get(selection3, idx);

                let val1 = unsafe { values1.get_unchecked(sel1) };
                let val2 = unsafe { values2.get_unchecked(sel2) };
                let val3 = unsafe { values3.get_unchecked(sel3) };

                output_buffer.idx = idx;
                op(val1, val2, val3, &mut output_buffer);
            }
        }

        let data = output_buffer.buffer.into_data();

        Ok(Array {
            datatype: builder.datatype,
            selection2: None,
            validity2: out_validity,
            data2: data,
            next: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::physical_type::{PhysicalI32, PhysicalUtf8};
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_arrays_eq;

    #[test]
    fn ternary_left_prepend_simple() {
        let strings = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let count = Array::try_from_iter([1, 2, 3]).unwrap();
        let pad = Array::try_from_iter(["<", ".", "!"]).unwrap();

        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Utf8, 3).unwrap();

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

        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Utf8, 3).unwrap();

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
        pad.select(&Arc::new(NopBufferManager), [1, 1, 0]).unwrap();

        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Utf8, 3).unwrap();

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
}
