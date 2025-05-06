use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::execution_format::{ExecutionFormat, SelectionFormat};
use crate::arrays::array::physical_type::{Addressable, MutableScalarStorage, ScalarStorage};
use crate::arrays::executor::{OutBuffer, PutBuffer};
use crate::util::iter::IntoExactSizeIterator;

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
        S: ScalarStorage,
        O: MutableScalarStorage,
        for<'a> Op: FnMut(&[&S::StorageType], PutBuffer<O::AddressableMut<'a>>),
    {
        // (selection, addressable)
        let inputs = arrays
            .iter()
            .map(|arr| {
                let format = match S::downcast_execution_format(&arr.data)? {
                    ExecutionFormat::Flat(buf) => SelectionFormat::flat(buf),
                    ExecutionFormat::Selection(buf) => buf,
                };
                Ok((format.selection, S::addressable(format.buffer)))
            })
            .collect::<Result<Vec<_>>>()?;

        let all_valid = arrays.iter().all(|arr| arr.validity.all_valid());
        let mut output = O::get_addressable_mut(out.buffer)?;
        let mut op_inputs = Vec::with_capacity(arrays.len());

        if all_valid {
            for (output_idx, input_idx) in sel.into_exact_size_iter().enumerate() {
                op_inputs.clear();
                for (sel, input) in &inputs {
                    let sel_idx = sel.get(input_idx).unwrap();
                    op_inputs.push(input.get(sel_idx).unwrap());
                }

                op(
                    &op_inputs,
                    PutBuffer::new(output_idx, &mut output, out.validity),
                );
            }
        } else {
            let validities: Vec<_> = arrays.iter().map(|arr| &arr.validity).collect();

            for (output_idx, input_idx) in sel.into_exact_size_iter().enumerate() {
                let all_valid = validities.iter().all(|v| v.is_valid(input_idx));

                if all_valid {
                    op_inputs.clear();
                    for (sel, input) in &inputs {
                        let sel_idx = sel.get(input_idx).unwrap();
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
    use super::*;
    use crate::arrays::array::physical_type::{PhysicalBool, PhysicalUtf8};
    use crate::arrays::datatype::DataType;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn uniform_and_simple() {
        let a = Array::try_from_iter([true, true, true]).unwrap();
        let b = Array::try_from_iter([true, true, false]).unwrap();
        let c = Array::try_from_iter([true, false, false]).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::boolean(), 3).unwrap();

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

        let mut out = Array::new(&DefaultBufferManager, DataType::utf8(), 3).unwrap();

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

        let mut out = Array::new(&DefaultBufferManager, DataType::utf8(), 3).unwrap();

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
        c.select(&DefaultBufferManager, [2, 2, 0]).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::utf8(), 3).unwrap();

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

    #[test]
    fn uniform_string_concat_row_wise_with_dictionary_invalid() {
        let a = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let b = Array::try_from_iter(["1", "2", "3"]).unwrap();
        let mut c = Array::try_from_iter([Some("dog"), None, Some("horse")]).unwrap();
        // '[NULL, "horse", "dog"]
        c.select(&DefaultBufferManager, [1, 2, 0]).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::utf8(), 3).unwrap();

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

        let expected = Array::try_from_iter([None, Some("b2horse"), Some("c3dog")]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn uniform_string_concat_row_wise_with_constant() {
        let a = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let b = Array::new_constant(&DefaultBufferManager, &"*".into(), 3).unwrap();
        let c = Array::try_from_iter(["dog", "cat", "horse"]).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::utf8(), 3).unwrap();

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

        let expected = Array::try_from_iter(["a*dog", "b*cat", "c*horse"]).unwrap();

        assert_arrays_eq(&expected, &out);
    }
}
