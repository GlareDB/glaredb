use stdutil::iter::IntoExactSizeIterator;
use rayexec_error::Result;

use crate::arrays::array::exp::Array;
use crate::arrays::array::flat::FlatArrayView;
use crate::arrays::buffer::physical_type::{Addressable, MutablePhysicalStorage, PhysicalStorage};
use crate::arrays::executor_exp::{OutBuffer, PutBuffer};

#[derive(Debug, Clone)]
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
            .map(|arr| S::get_addressable(arr.data()))
            .collect::<Result<Vec<_>>>()?;

        let all_valid = arrays.iter().all(|arr| arr.validity().all_valid());

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
            let validities: Vec<_> = arrays.iter().map(|arr| arr.validity()).collect();

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
            .map(|arr| S::get_addressable(&arr.array_buffer))
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
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::buffer::buffer_manager::NopBufferManager;
    use crate::arrays::buffer::physical_type::{PhysicalBool, PhysicalUtf8};
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_arrays_eq;

    #[test]
    fn uniform_and_simple() {
        let a = Array::try_from_iter([true, true, true]).unwrap();
        let b = Array::try_from_iter([true, true, false]).unwrap();
        let c = Array::try_from_iter([true, false, false]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();

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

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 3).unwrap();

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

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 3).unwrap();

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
        c.select(&NopBufferManager, [2, 2, 0]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 3).unwrap();

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
