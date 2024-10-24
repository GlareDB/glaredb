use rayexec_error::{RayexecError, Result};

use super::check_validity;
use crate::array::Array;
use crate::bitmap::Bitmap;
use crate::executor::builder::{ArrayBuilder, ArrayDataBuffer, OutputBuffer};
use crate::executor::physical_type::PhysicalStorage;
use crate::executor::scalar::validate_logical_len;
use crate::selection;
use crate::storage::AddressableStorage;

#[derive(Debug, Clone, Copy)]
pub struct UniformExecutor;

impl UniformExecutor {
    pub fn execute<'a, S, B, Op>(
        arrays: &[&'a Array],
        builder: ArrayBuilder<B>,
        mut op: Op,
    ) -> Result<Array>
    where
        Op: FnMut(&[<S::Storage as AddressableStorage>::T], &mut OutputBuffer<B>),
        S: PhysicalStorage<'a>,
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
                .map(|a| S::get_storage(&a.data))
                .collect::<Result<Vec<_>>>()?;

            let validities: Vec<_> = arrays.iter().map(|a| a.validity()).collect();

            let mut out_validity_builder = Bitmap::new_with_all_true(len);

            for idx in 0..len {
                op_inputs.clear();
                let mut row_invalid = false;
                for arr_idx in 0..arrays.len() {
                    let sel = selection::get_unchecked(selections[arr_idx], idx);
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
                .map(|a| S::get_storage(&a.data))
                .collect::<Result<Vec<_>>>()?;

            for idx in 0..len {
                op_inputs.clear();
                for arr_idx in 0..arrays.len() {
                    let sel = selection::get_unchecked(selections[arr_idx], idx);
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
            selection: None,
            validity: out_validity,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use selection::SelectionVector;

    use super::*;
    use crate::datatype::DataType;
    use crate::executor::builder::GermanVarlenBuffer;
    use crate::executor::physical_type::PhysicalUtf8;
    use crate::scalar::ScalarValue;

    #[test]
    fn uniform_string_concat_row_wise() {
        let first = Array::from_iter(["a", "b", "c"]);
        let second = Array::from_iter(["1", "2", "3"]);
        let third = Array::from_iter(["dog", "cat", "horse"]);

        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(3),
        };

        let mut string_buffer = String::new();

        let got = UniformExecutor::execute::<PhysicalUtf8, _, _>(
            &[&first, &second, &third],
            builder,
            |inputs, buf| {
                string_buffer.clear();
                for input in inputs {
                    string_buffer.push_str(input);
                }
                buf.put(string_buffer.as_str())
            },
        )
        .unwrap();

        assert_eq!(ScalarValue::from("a1dog"), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from("b2cat"), got.physical_scalar(1).unwrap());
        assert_eq!(
            ScalarValue::from("c3horse"),
            got.physical_scalar(2).unwrap()
        );
    }

    #[test]
    fn uniform_string_concat_row_wise_with_invalid() {
        let first = Array::from_iter(["a", "b", "c"]);
        let mut second = Array::from_iter(["1", "2", "3"]);
        second.set_physical_validity(1, false); // "2" => NULL
        let third = Array::from_iter(["dog", "cat", "horse"]);

        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(3),
        };

        let mut string_buffer = String::new();

        let got = UniformExecutor::execute::<PhysicalUtf8, _, _>(
            &[&first, &second, &third],
            builder,
            |inputs, buf| {
                string_buffer.clear();
                for input in inputs {
                    string_buffer.push_str(input);
                }
                buf.put(string_buffer.as_str())
            },
        )
        .unwrap();

        assert_eq!(ScalarValue::from("a1dog"), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::Null, got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from("c3horse"), got.logical_value(2).unwrap());
    }

    #[test]
    fn uniform_string_concat_row_wise_with_invalid_and_reordered() {
        let first = Array::from_iter(["a", "b", "c"]);
        let mut second = Array::from_iter(["1", "2", "3"]);
        second.select_mut(SelectionVector::from_iter([1, 0, 2])); // ["1", "2", "3"] => ["2", "1", "3"]
        second.set_physical_validity(1, false); // "2" => NULL, referencing physical index
        let third = Array::from_iter(["dog", "cat", "horse"]);

        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(3),
        };

        let mut string_buffer = String::new();

        let got = UniformExecutor::execute::<PhysicalUtf8, _, _>(
            &[&first, &second, &third],
            builder,
            |inputs, buf| {
                string_buffer.clear();
                for input in inputs {
                    string_buffer.push_str(input);
                }
                buf.put(string_buffer.as_str())
            },
        )
        .unwrap();

        assert_eq!(ScalarValue::Null, got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from("b1cat"), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from("c3horse"), got.logical_value(2).unwrap());
    }
}
