use rayexec_error::{RayexecError, Result};
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
pub struct UniformExecutor;

impl UniformExecutor {
    /// Executes an operation across uniform array types.
    ///
    /// The selection applies to all arrays.
    pub fn execute<S, O, Op>(
        arrays: &[Array],
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer,
        mut op: Op,
    ) -> Result<()>
    where
        S: PhysicalStorage,
        O: MutablePhysicalStorage,
        for<'a> Op: FnMut(&[&S::StorageType], PutBuffer<O::AddressableMut<'a>>),
    {
        if arrays.iter().any(|arr| arr.is_dictionary()) {
            let flats = arrays
                .iter()
                .map(|arr| arr.flat_view())
                .collect::<Result<Vec<_>>>()?;

            return Self::execute_flat::<S, O, Op>(&flats, sel, out, op);
        }

        let inputs = arrays
            .iter()
            .map(|arr| S::get_addressable(&arr.next().data))
            .collect::<Result<Vec<_>>>()?;

        let all_valid = arrays.iter().all(|arr| arr.next().validity.all_valid());

        let mut output = O::get_addressable_mut(out.buffer)?;

        let mut op_inputs = Vec::with_capacity(arrays.len());

        if all_valid {
            for (output_idx, input_idx) in sel.into_iter().enumerate() {
                op_inputs.clear();
                for input in &inputs {
                    op_inputs.push(input.get(input_idx).unwrap());
                }

                op(
                    &op_inputs,
                    PutBuffer::new(output_idx, &mut output, out.validity),
                );
            }
        } else {
            let validities: Vec<_> = arrays.iter().map(|arr| &arr.next().validity).collect();

            for (output_idx, input_idx) in sel.into_iter().enumerate() {
                let all_valid = validities.iter().all(|v| v.is_valid(input_idx));

                if all_valid {
                    op_inputs.clear();
                    for input in &inputs {
                        op_inputs.push(input.get(input_idx).unwrap());
                    }

                    op(
                        &op_inputs,
                        PutBuffer::new(output_idx, &mut output, out.validity),
                    );
                } else {
                    out.validity.set_invalid(output_idx);
                }
            }
        }

        Ok(())
    }

    pub fn execute_flat<S, O, Op>(
        arrays: &[FlatArrayView],
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer,
        mut op: Op,
    ) -> Result<()>
    where
        S: PhysicalStorage,
        O: MutablePhysicalStorage,
        for<'a> Op: FnMut(&[&S::StorageType], PutBuffer<O::AddressableMut<'a>>),
    {
        // TODO: length check

        let inputs = arrays
            .iter()
            .map(|arr| S::get_addressable(arr.array_buffer))
            .collect::<Result<Vec<_>>>()?;

        let all_valid = arrays.iter().all(|arr| arr.validity.all_valid());

        let mut output = O::get_addressable_mut(out.buffer)?;

        let mut op_inputs = Vec::with_capacity(arrays.len());

        if all_valid {
            for (output_idx, input_idx) in sel.into_iter().enumerate() {
                op_inputs.clear();
                for (input, array) in inputs.iter().zip(arrays) {
                    let sel_idx = array.selection.get(input_idx).unwrap();
                    op_inputs.push(input.get(sel_idx).unwrap());
                }

                op(
                    &op_inputs,
                    PutBuffer::new(output_idx, &mut output, out.validity),
                );
            }
        } else {
            for (output_idx, input_idx) in sel.into_iter().enumerate() {
                let mut all_valid = true;

                for array in arrays {
                    let sel_idx = array.selection.get(input_idx).unwrap();
                    all_valid = all_valid && array.validity.is_valid(sel_idx);
                }

                if all_valid {
                    for (input, array) in inputs.iter().zip(arrays) {
                        let sel_idx = array.selection.get(input_idx).unwrap();
                        op_inputs.push(input.get(sel_idx).unwrap());
                    }

                    op(
                        &op_inputs,
                        PutBuffer::new(output_idx, &mut output, out.validity),
                    );
                } else {
                    out.validity.set_invalid(output_idx);
                }
            }
        }

        Ok(())
    }

    pub fn execute2<'a, S, B, Op>(
        arrays: &[&'a Array],
        builder: ArrayBuilder<B>,
        mut op: Op,
    ) -> Result<Array>
    where
        Op: FnMut(&[S::Type<'a>], &mut OutputBuffer<B>),
        S: PhysicalStorage,
        B: ArrayDataBuffer,
    {
        let len = match arrays.first() {
            Some(first) => validate_logical_len(&builder.buffer, first)?,
            None => return Err(RayexecError::new("Cannot execute on no arrays")),
        };

        for arr in arrays {
            let _ = validate_logical_len(&builder.buffer, arr)?;
        }

        let any_invalid = arrays.iter().any(|a| a.validity().is_some());

        let selections: Vec<_> = arrays.iter().map(|a| a.selection_vector()).collect();

        let mut out_validity = None;
        let mut output_buffer = OutputBuffer {
            idx: 0,
            buffer: builder.buffer,
        };

        let mut op_inputs = Vec::with_capacity(arrays.len());

        if any_invalid {
            let storage_values: Vec<_> = arrays
                .iter()
                .map(|a| S::get_storage(&a.data2))
                .collect::<Result<Vec<_>>>()?;

            let validities: Vec<_> = arrays.iter().map(|a| a.validity()).collect();

            let mut out_validity_builder = Bitmap::new_with_all_true(len);

            for idx in 0..len {
                op_inputs.clear();
                let mut row_invalid = false;
                for arr_idx in 0..arrays.len() {
                    let sel = selection::get(selections[arr_idx], idx);
                    if row_invalid || !check_validity(sel, validities[arr_idx]) {
                        row_invalid = true;
                        out_validity_builder.set_unchecked(idx, false);
                        continue;
                    }

                    let val = unsafe { storage_values[arr_idx].get_unchecked(sel) };
                    op_inputs.push(val);
                }

                output_buffer.idx = idx;
                op(op_inputs.as_slice(), &mut output_buffer);
            }

            out_validity = Some(out_validity_builder.into());
        } else {
            let storage_values: Vec<_> = arrays
                .iter()
                .map(|a| S::get_storage(&a.data2))
                .collect::<Result<Vec<_>>>()?;

            for idx in 0..len {
                op_inputs.clear();
                for arr_idx in 0..arrays.len() {
                    let sel = selection::get(selections[arr_idx], idx);
                    let val = unsafe { storage_values[arr_idx].get_unchecked(sel) };
                    op_inputs.push(val);
                }

                output_buffer.idx = idx;
                op(op_inputs.as_slice(), &mut output_buffer);
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
    use crate::arrays::array::physical_type::{PhysicalBool, PhysicalUtf8};
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_arrays_eq;

    #[test]
    fn uniform_and_simple() {
        let a = Array::try_from_iter([true, true, true]).unwrap();
        let b = Array::try_from_iter([true, true, false]).unwrap();
        let c = Array::try_from_iter([true, false, false]).unwrap();

        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Boolean, 3).unwrap();

        UniformExecutor::execute::<PhysicalBool, PhysicalBool, _>(
            &[a, b, c],
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |bools, buf| {
                let v = bools.iter().all(|b| **b);
                buf.put(&v);
            },
        )
        .unwrap();

        let expected = Array::try_from_iter([true, false, false]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn uniform_string_concat_row_wise() {
        let a = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let b = Array::try_from_iter(["1", "2", "3"]).unwrap();
        let c = Array::try_from_iter(["dog", "cat", "horse"]).unwrap();

        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Utf8, 3).unwrap();

        let mut str_buf = String::new();

        UniformExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            &[a, b, c],
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |strings, buf| {
                str_buf.clear();
                for s in strings {
                    str_buf.push_str(s);
                }
                buf.put(&str_buf);
            },
        )
        .unwrap();

        let expected = Array::try_from_iter(["a1dog", "b2cat", "c3horse"]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn uniform_string_concat_row_wise_with_invalid() {
        let a = Array::try_from_iter([Some("a"), Some("b"), None]).unwrap();
        let b = Array::try_from_iter(["1", "2", "3"]).unwrap();
        let c = Array::try_from_iter([Some("dog"), None, Some("horse")]).unwrap();

        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Utf8, 3).unwrap();

        let mut str_buf = String::new();

        UniformExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            &[a, b, c],
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |strings, buf| {
                str_buf.clear();
                for s in strings {
                    str_buf.push_str(s);
                }
                buf.put(&str_buf);
            },
        )
        .unwrap();

        let expected = Array::try_from_iter([Some("a1dog"), None, None]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn uniform_string_concat_row_wise_with_dictionary() {
        let a = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let b = Array::try_from_iter(["1", "2", "3"]).unwrap();
        let mut c = Array::try_from_iter(["dog", "cat", "horse"]).unwrap();
        // '["horse", "horse", "dog"]
        c.select(&Arc::new(NopBufferManager), [2, 2, 0]).unwrap();

        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Utf8, 3).unwrap();

        let mut str_buf = String::new();

        UniformExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            &[a, b, c],
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |strings, buf| {
                str_buf.clear();
                for s in strings {
                    str_buf.push_str(s);
                }
                buf.put(&str_buf);
            },
        )
        .unwrap();

        let expected = Array::try_from_iter(["a1horse", "b2horse", "c3dog"]).unwrap();

        assert_arrays_eq(&expected, &out);
    }
}
