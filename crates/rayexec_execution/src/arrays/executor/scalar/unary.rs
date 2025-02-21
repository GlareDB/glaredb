use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use crate::buffer::buffer_manager::BufferManager;
use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{
    Addressable,
    AddressableMut,
    MutableScalarStorage,
    PhysicalBool,
    ScalarStorage,
};
use crate::arrays::array::Array;
use crate::arrays::executor::{OutBuffer, PutBuffer};

#[derive(Debug, Clone)]
pub struct UnaryExecutor;

impl UnaryExecutor {
    /// Execute a unary operation on `array`, placing results in `out`.
    pub fn execute<S, O, Op, B>(
        array: &Array<B>,
        selection: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer<B>,
        mut op: Op,
    ) -> Result<()>
    where
        S: ScalarStorage,
        O: MutableScalarStorage,
        for<'a> Op: FnMut(&S::StorageType, PutBuffer<O::AddressableMut<'a, B>, B>),
        B: BufferManager,
    {
        if array.should_flatten_for_execution() {
            let view = array.flatten()?;
            return Self::execute_flat::<S, _, _, _>(view, selection, out, op);
        }

        let input = S::get_addressable(&array.data)?;
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

    pub fn execute_flat<S, O, Op, B>(
        array: FlattenedArray<'_, B>,
        selection: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer<B>,
        mut op: Op,
    ) -> Result<()>
    where
        S: ScalarStorage,
        O: MutableScalarStorage,
        for<'b> Op: FnMut(&S::StorageType, PutBuffer<O::AddressableMut<'b, B>, B>),
        B: BufferManager,
    {
        let input = S::get_addressable(array.array_buffer)?;
        let mut output = O::get_addressable_mut(out.buffer)?;

        let validity = array.validity;

        if validity.all_valid() {
            for (output_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                let selected_idx = array.selection.get(input_idx).unwrap();

                op(
                    input.get(selected_idx).unwrap(),
                    PutBuffer::new(output_idx, &mut output, out.validity),
                );
            }
        } else {
            for (output_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                let selected_idx = array.selection.get(input_idx).unwrap();

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

    /// Iterate over all values in a flat array view, calling `op` for each row.
    ///
    /// Valid values are represented with Some, invalid values are represented
    /// with None.
    ///
    /// `op` will be called with the output index for that row.
    pub fn for_each_flat<S, Op>(
        array: FlattenedArray<'_>,
        selection: impl IntoExactSizeIterator<Item = usize>,
        mut op: Op,
    ) -> Result<()>
    where
        S: ScalarStorage,
        Op: FnMut(usize, Option<&S::StorageType>),
    {
        let input = S::get_addressable(array.array_buffer)?;
        let validity = array.validity;

        // TODO: `op` should be called with input_idx?
        if validity.all_valid() {
            for (output_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                let selected_idx = array.selection.get(input_idx).unwrap();
                let v = input.get(selected_idx).unwrap();

                op(output_idx, Some(v))
            }
        } else {
            for (output_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                let selected_idx = array.selection.get(input_idx).unwrap();

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

    pub fn select(
        array: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        true_indices: &mut Vec<usize>,
    ) -> Result<()> {
        let flat = array.flatten()?;

        let bools = PhysicalBool::get_addressable(flat.array_buffer)?;
        let validity = flat.validity;

        if validity.all_valid() {
            for input_idx in selection.into_exact_size_iter() {
                let selected_idx = flat.selection.get(input_idx).unwrap();
                let v = *bools.get(selected_idx).unwrap();

                if v {
                    true_indices.push(input_idx);
                }
            }
        } else {
            for input_idx in selection.into_exact_size_iter() {
                let selected_idx = flat.selection.get(input_idx).unwrap();

                if validity.is_valid(selected_idx) {
                    let v = *bools.get(selected_idx).unwrap();
                    if v {
                        true_indices.push(input_idx);
                    }
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
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::arrays::array::physical_type::{
        PhysicalI32,
        PhysicalUtf8,
        StringViewAddressableMut,
    };
    use crate::arrays::array::Array;
    use crate::arrays::datatype::DataType;
    use crate::testutil::arrays::assert_arrays_eq;

    #[test]
    fn int32_inc_by_2() {
        let array = Array::try_from_iter([1, 2, 3]).unwrap();
        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _, _>(
            &array,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();
        assert!(out.validity.all_valid());

        let out_slice = out
            .data
            .get_scalar_buffer()
            .unwrap()
            .try_as_slice::<PhysicalI32>()
            .unwrap();
        assert_eq!(&[3, 4, 5], out_slice);
    }

    #[test]
    fn int32_inc_by_2_on_selection() {
        let mut array = Array::try_from_iter([1, 2, 3]).unwrap();
        // => [2, 3, 1]
        array.select(&NopBufferManager, [1, 2, 0]).unwrap();
        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _, _>(
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
        array.select(&NopBufferManager, [1, 2, 0]).unwrap();
        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _, _>(
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
        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        let flat = FlattenedArray::from_array(&array).unwrap();

        UnaryExecutor::execute_flat::<PhysicalI32, PhysicalI32, _, _>(
            flat,
            0..3,
            OutBuffer::from_array(&mut out).unwrap(),
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();

        assert!(out.validity.all_valid());

        let out_slice = out
            .data
            .get_scalar_buffer()
            .unwrap()
            .try_as_slice::<PhysicalI32>()
            .unwrap();
        assert_eq!(&[3, 4, 5], out_slice);
    }

    #[test]
    fn int32_inc_by_2_in_place() {
        let mut array = Array::try_from_iter([1, 2, 3]).unwrap();

        UnaryExecutor::execute_in_place::<PhysicalI32, _>(&mut array, 0..3, |v| *v += 2).unwrap();

        let arr_slice = array
            .data
            .get_scalar_buffer()
            .unwrap()
            .try_as_slice::<PhysicalI32>()
            .unwrap();
        assert_eq!(&[3, 4, 5], arr_slice);
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

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 6).unwrap();

        fn my_string_double<B: BufferManager>(
            s: &str,
            buf: PutBuffer<StringViewAddressableMut<B>, B>,
        ) {
            let mut double = s.to_string();
            double.push_str(s);
            buf.put(&double);
        }

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
            &array,
            0..6,
            OutBuffer::from_array(&mut out).unwrap(),
            my_string_double,
        )
        .unwrap();
        assert!(out.validity.all_valid());

        let out = out
            .data
            .get_string_buffer()
            .unwrap()
            .try_as_string_view()
            .unwrap();

        assert_eq!("aa", out.get(0).unwrap());
        assert_eq!("bbbb", out.get(1).unwrap());
        assert_eq!("cccccc", out.get(2).unwrap());
        assert_eq!("dddddddd", out.get(3).unwrap());
        assert_eq!("heapafterheapafter", out.get(4).unwrap());
        assert_eq!(
            "alongerstringdontinlinealongerstringdontinline",
            out.get(5).unwrap()
        );
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

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 6).unwrap();

        let mut string_buf = String::new();

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
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

        let out = out
            .data
            .get_string_buffer()
            .unwrap()
            .try_as_string_view()
            .unwrap();

        assert_eq!("aa", out.get(0).unwrap());
        assert_eq!("bbbb", out.get(1).unwrap());
        assert_eq!("cccccc", out.get(2).unwrap());
        assert_eq!("dddddddd", out.get(3).unwrap());
        assert_eq!("heapafterheapafter", out.get(4).unwrap());
        assert_eq!(
            "alongerstringdontinlinealongerstringdontinline",
            out.get(5).unwrap()
        );
    }

    #[test]
    fn string_uppercase_in_place() {
        let mut array = Array::try_from_iter(["a", "bb", "ccc"]).unwrap();

        UnaryExecutor::execute_in_place::<PhysicalUtf8, _>(&mut array, 0..3, |v| {
            v.make_ascii_uppercase()
        })
        .unwrap();

        let out = array
            .data
            .get_string_buffer()
            .unwrap()
            .try_as_string_view()
            .unwrap();

        assert_eq!("A", out.get(0).unwrap());
        assert_eq!("BB", out.get(1).unwrap());
        assert_eq!("CCC", out.get(2).unwrap());
    }

    #[test]
    fn int32_inc_by_2_with_dict() {
        let mut array = Array::try_from_iter([1, 2, 3]).unwrap();
        // [3, 3, 2, 1, 1, 3]
        array.select(&NopBufferManager, [2, 2, 1, 0, 0, 2]).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 6).unwrap();

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _, _>(
            &array,
            0..6,
            OutBuffer::from_array(&mut out).unwrap(),
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();
        assert!(out.validity.all_valid());

        let out_slice = out
            .data
            .get_scalar_buffer()
            .unwrap()
            .try_as_slice::<PhysicalI32>()
            .unwrap();
        assert_eq!(&[5, 5, 4, 3, 3, 5], out_slice);
    }

    #[test]
    fn int32_inc_by_2_constant() {
        let array = Array::new_constant(&NopBufferManager, &3.into(), 2).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 2).unwrap();

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _, _>(
            &array,
            0..2,
            OutBuffer::from_array(&mut out).unwrap(),
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();

        let expected = Array::try_from_iter([5, 5]).unwrap();
        assert_arrays_eq(&expected, &out);
    }
}
