use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::execution_format::{ExecutionFormat, SelectionFormat};
use crate::arrays::array::physical_type::{
    Addressable, AddressableMut, MutableScalarStorage, PhysicalBool, ScalarStorage,
};
use crate::arrays::executor::{OutBuffer, PutBuffer};
use crate::util::iter::IntoExactSizeIterator;

#[derive(Debug, Clone)]
pub struct UnaryExecutor;

impl UnaryExecutor {
    /// Execute a unary operation on `array`, placing results in `out`.
    pub fn execute<S, O, Op>(
        array: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer,
        mut op: Op,
    ) -> Result<()>
    where
        S: ScalarStorage,
        O: MutableScalarStorage,
        for<'a> Op: FnMut(&S::StorageType, PutBuffer<O::AddressableMut<'a>>),
    {
        match S::downcast_execution_format(&array.data)? {
            ExecutionFormat::Flat(buffer) => {
                let input = S::addressable(buffer);
                let mut output = O::get_addressable_mut(out.buffer)?;

                let validity = &array.validity;

                if validity.all_valid() {
                    for (output_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                        op(
                            input.get(input_idx).unwrap(),
                            PutBuffer::new(output_idx, &mut output, out.validity),
                        );
                    }
                } else {
                    for (output_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                        if validity.is_valid(input_idx) {
                            op(
                                input.get(input_idx).unwrap(),
                                PutBuffer::new(output_idx, &mut output, out.validity),
                            );
                        } else {
                            out.validity.set_invalid(output_idx);
                        }
                    }
                }

                Ok(())
            }
            ExecutionFormat::Selection(buffer) => {
                let input = S::addressable(buffer.buffer);
                let array_selection = buffer.selection;
                let mut output = O::get_addressable_mut(out.buffer)?;

                let validity = &array.validity;

                if validity.all_valid() {
                    for (output_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                        let selected_idx = array_selection.get(input_idx).unwrap();

                        op(
                            input.get(selected_idx).unwrap(),
                            PutBuffer::new(output_idx, &mut output, out.validity),
                        );
                    }
                } else {
                    for (output_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                        let selected_idx = array_selection.get(input_idx).unwrap();

                        if validity.is_valid(input_idx) {
                            op(
                                input.get(selected_idx).unwrap(),
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
    }

    /// Iterate over all values in a flat array view, calling `op` for each row.
    ///
    /// Valid values are represented with Some, invalid values are represented
    /// with None.
    ///
    /// `op` will be called with the output index for that row.
    pub fn for_each_flat<S, Op>(
        array: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        mut op: Op,
    ) -> Result<()>
    where
        S: ScalarStorage,
        Op: FnMut(usize, Option<&S::StorageType>),
    {
        let validity = &array.validity;
        let buffer = match S::downcast_execution_format(&array.data)? {
            ExecutionFormat::Flat(flat) => SelectionFormat::flat(flat),
            ExecutionFormat::Selection(buf) => buf,
        };
        let input = S::addressable(buffer.buffer);

        // TODO: `op` should be called with input_idx?
        if validity.all_valid() {
            for (output_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                let selected_idx = buffer.selection.get(input_idx).unwrap();
                let v = input.get(selected_idx).unwrap();

                op(output_idx, Some(v))
            }
        } else {
            for (output_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                let selected_idx = buffer.selection.get(input_idx).unwrap();

                if validity.is_valid(selected_idx) {
                    let v = input.get(selected_idx).unwrap();
                    op(output_idx, Some(v));
                } else {
                    op(output_idx, None);
                }
            }
        }

        Ok(())
    }

    /// Executes an operation in place.
    ///
    /// Note that changing the lengths for variable length data is not yet
    /// supported, as the length change won't persist since the metadata isn't
    /// being changed.
    pub fn execute_in_place<S, Op>(
        array: &mut Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        mut op: Op,
    ) -> Result<()>
    where
        S: MutableScalarStorage,
        Op: FnMut(&mut S::StorageType),
    {
        let validity = &array.validity;
        let mut input = S::get_addressable_mut(&mut array.data)?;

        if validity.all_valid() {
            for idx in selection.into_iter() {
                op(input.get_mut(idx).unwrap());
            }
        } else {
            for idx in selection.into_iter() {
                if validity.is_valid(idx) {
                    op(input.get_mut(idx).unwrap());
                }
            }
        }

        Ok(())
    }

    /// Iterate over a boolean array, calling `when_true` and `when_false`
    /// depending on the value.
    ///
    /// If a value is NULL, `when_false` is called.
    ///
    /// `when_true` and `when_false` are called with the logical index for the
    /// array.
    pub fn select(
        array: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        mut when_true: impl FnMut(usize),
        mut when_false: impl FnMut(usize),
    ) -> Result<()> {
        let validity = &array.validity;
        let buffer = match PhysicalBool::downcast_execution_format(&array.data)? {
            ExecutionFormat::Flat(flat) => SelectionFormat::flat(flat),
            ExecutionFormat::Selection(buf) => buf,
        };
        let input = PhysicalBool::addressable(buffer.buffer);

        if validity.all_valid() {
            for input_idx in selection.into_exact_size_iter() {
                let selected_idx = buffer.selection.get(input_idx).unwrap();
                let v = *input.get(selected_idx).unwrap();

                if v {
                    when_true(input_idx)
                } else {
                    when_false(input_idx)
                }
            }
        } else {
            for input_idx in selection.into_exact_size_iter() {
                let valid = validity.is_valid(input_idx);
                let selected_idx = buffer.selection.get(input_idx).unwrap();

                if valid {
                    let v = *input.get(selected_idx).unwrap();
                    if v {
                        when_true(input_idx);
                    } else {
                        when_false(input_idx);
                    }
                } else {
                    when_false(input_idx);
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
    use crate::arrays::array::physical_type::{
        PhysicalI32, PhysicalUtf8, StringViewAddressableMut,
    };
    use crate::arrays::datatype::DataType;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::generate_array;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn int32_inc_by_2() {
        let array = Array::try_from_iter([1, 2, 3]).unwrap();
        let mut out = Array::new(&DefaultBufferManager, DataType::int32(), 3).unwrap();

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _>(
            &array,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();
        assert!(out.validity.all_valid());

        let expected = generate_array!([3, 4, 5]);
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn int32_inc_by_2_on_selection() {
        let mut array = Array::try_from_iter([1, 2, 3]).unwrap();
        // => [2, 3, 1]
        array.select(&DefaultBufferManager, [1, 2, 0]).unwrap();
        let mut out = Array::new(&DefaultBufferManager, DataType::int32(), 3).unwrap();

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _>(
            &array,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();

        let expected = Array::try_from_iter([4, 5, 3]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn int32_inc_by_2_on_selection_with_invalid() {
        let mut array = Array::try_from_iter([Some(1), None, Some(3)]).unwrap();
        // => [NULL, 3, 1]
        array.select(&DefaultBufferManager, [1, 2, 0]).unwrap();
        let mut out = Array::new(&DefaultBufferManager, DataType::int32(), 3).unwrap();

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _>(
            &array,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();

        let expected = Array::try_from_iter([None, Some(5), Some(3)]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn int32_inc_by_2_using_flattened_array() {
        let array = Array::try_from_iter([1, 2, 3]).unwrap();
        let mut out = Array::new(&DefaultBufferManager, DataType::int32(), 3).unwrap();

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _>(
            &array,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();

        assert!(out.validity.all_valid());

        let expected = generate_array!([3, 4, 5]);
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn int32_inc_by_2_in_place() {
        let mut array = Array::try_from_iter([1, 2, 3]).unwrap();

        UnaryExecutor::execute_in_place::<PhysicalI32, _>(&mut array, 0..3, |v| *v += 2).unwrap();

        let expected = generate_array!([3, 4, 5]);
        assert_arrays_eq(&expected, &array);
    }

    #[test]
    fn string_double_named_func() {
        // Example with defined function, and allocating a new string every time.
        let array = Array::try_from_iter([
            "a",
            "bb",
            "ccc",
            "dddd",
            "heapafter", // Inlined, will be moved to heap after doubling.
            "alongerstringdontinline",
        ])
        .unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::utf8(), 6).unwrap();

        fn my_string_double(s: &str, buf: PutBuffer<StringViewAddressableMut>) {
            let mut double = s.to_string();
            double.push_str(s);
            buf.put(&double);
        }

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            &array,
            0..6,
            OutBuffer::from_array(&mut out).unwrap(),
            my_string_double,
        )
        .unwrap();
        assert!(out.validity.all_valid());

        let expected = generate_array!([
            "aa",
            "bbbb",
            "cccccc",
            "dddddddd",
            "heapafterheapafter",
            "alongerstringdontinlinealongerstringdontinline",
        ]);
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn string_double_closure_reused_buf() {
        // Same thing, but with closure reusing a string buffer.
        let array = Array::try_from_iter([
            "a",
            "bb",
            "ccc",
            "dddd",
            "heapafter", // Inlined, will be moved to heap after doubling.
            "alongerstringdontinline",
        ])
        .unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::utf8(), 6).unwrap();

        let mut string_buf = String::new();

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            &array,
            0..6,
            OutBuffer::from_array(&mut out).unwrap(),
            |s, buf| {
                string_buf.clear();

                string_buf.push_str(s);
                string_buf.push_str(s);

                buf.put(&string_buf);
            },
        )
        .unwrap();
        assert!(out.validity.all_valid());

        let expected = generate_array!([
            "aa",
            "bbbb",
            "cccccc",
            "dddddddd",
            "heapafterheapafter",
            "alongerstringdontinlinealongerstringdontinline",
        ]);
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn string_uppercase_in_place() {
        let mut array = Array::try_from_iter(["a", "bb", "ccc"]).unwrap();

        UnaryExecutor::execute_in_place::<PhysicalUtf8, _>(&mut array, 0..3, |v| {
            v.make_ascii_uppercase()
        })
        .unwrap();

        let expected = generate_array!(["A", "BB", "CCC"]);
        assert_arrays_eq(&expected, &array);
    }

    #[test]
    fn int32_inc_by_2_with_dict() {
        let mut array = Array::try_from_iter([1, 2, 3]).unwrap();
        // [3, 3, 2, 1, 1, 3]
        array
            .select(&DefaultBufferManager, [2, 2, 1, 0, 0, 2])
            .unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::int32(), 6).unwrap();

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _>(
            &array,
            0..6,
            OutBuffer::from_array(&mut out).unwrap(),
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();
        assert!(out.validity.all_valid());

        let expected = generate_array!([5, 5, 4, 3, 3, 5]);
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn int32_inc_by_2_constant() {
        let array = Array::new_constant(&DefaultBufferManager, &3.into(), 2).unwrap();

        let mut out = Array::new(&DefaultBufferManager, DataType::int32(), 2).unwrap();

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _>(
            &array,
            0..2,
            OutBuffer::from_array(&mut out).unwrap(),
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();

        let expected = Array::try_from_iter([5, 5]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn select_simple() {
        let array = generate_array!([Some(true), Some(false), None, Some(true)]);

        let mut trues = Vec::new();
        let mut falses = Vec::new();

        UnaryExecutor::select(&array, 0..4, |idx| trues.push(idx), |idx| falses.push(idx)).unwrap();

        assert_eq!(&[0, 3], trues.as_slice());
        assert_eq!(&[1, 2], falses.as_slice());
    }

    #[test]
    fn select_dictionary() {
        let mut array = generate_array!([Some(true), Some(false), None, Some(true)]);
        // [NULL, NULL, false, true]
        array.select(&DefaultBufferManager, [2, 2, 1, 0]).unwrap();

        let mut trues = Vec::new();
        let mut falses = Vec::new();

        UnaryExecutor::select(&array, 0..4, |idx| trues.push(idx), |idx| falses.push(idx)).unwrap();

        assert_eq!(&[3], trues.as_slice());
        assert_eq!(&[0, 1, 2], falses.as_slice());
    }
}
