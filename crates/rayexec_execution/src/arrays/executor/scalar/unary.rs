use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use super::validate_logical_len;
use crate::arrays::array::flat::FlatArrayView;
use crate::arrays::array::physical_type::{
    Addressable,
    AddressableMut,
    MutablePhysicalStorage,
    PhysicalBool,
    PhysicalStorage,
};
use crate::arrays::array::Array;
use crate::arrays::bitmap::Bitmap;
use crate::arrays::executor::builder::{ArrayBuilder, ArrayDataBuffer, OutputBuffer};
use crate::arrays::executor::{OutBuffer, PutBuffer};
use crate::arrays::selection;
use crate::arrays::storage::AddressableStorage;

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
        S: PhysicalStorage,
        O: MutablePhysicalStorage,
        for<'a> Op: FnMut(&S::StorageType, PutBuffer<O::AddressableMut<'a>>),
    {
        if array.is_dictionary() {
            let view = array.flat_view()?;
            return Self::execute_flat::<S, _, _>(view, selection, out, op);
        }

        let input = S::get_addressable(&array.next.as_ref().unwrap().data)?;
        let mut output = O::get_addressable_mut(out.buffer)?;

        let validity = &array.next.as_ref().unwrap().validity;

        if validity.all_valid() {
            for (output_idx, input_idx) in selection.into_iter().enumerate() {
                op(
                    input.get(input_idx).unwrap(),
                    PutBuffer::new(output_idx, &mut output, out.validity),
                );
            }
        } else {
            for (output_idx, input_idx) in selection.into_iter().enumerate() {
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

    pub fn execute_flat<S, O, Op>(
        array: FlatArrayView<'_>,
        selection: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer,
        mut op: Op,
    ) -> Result<()>
    where
        S: PhysicalStorage,
        O: MutablePhysicalStorage,
        for<'b> Op: FnMut(&S::StorageType, PutBuffer<O::AddressableMut<'b>>),
    {
        let input = S::get_addressable(array.array_buffer)?;
        let mut output = O::get_addressable_mut(out.buffer)?;

        let validity = array.validity;

        if validity.all_valid() {
            for (output_idx, input_idx) in selection.into_iter().enumerate() {
                let selected_idx = array.selection.get(input_idx).unwrap();

                op(
                    input.get(selected_idx).unwrap(),
                    PutBuffer::new(output_idx, &mut output, out.validity),
                );
            }
        } else {
            for (output_idx, input_idx) in selection.into_iter().enumerate() {
                let selected_idx = array.selection.get(input_idx).unwrap();

                if validity.is_valid(selected_idx) {
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
        array: FlatArrayView<'_>,
        selection: impl IntoExactSizeIterator<Item = usize>,
        mut op: Op,
    ) -> Result<()>
    where
        S: PhysicalStorage,
        Op: FnMut(usize, Option<&S::StorageType>),
    {
        let input = S::get_addressable(array.array_buffer)?;
        let validity = array.validity;

        // TODO: `op` should be called with input_idx?
        if validity.all_valid() {
            for (output_idx, input_idx) in selection.into_iter().enumerate() {
                let selected_idx = array.selection.get(input_idx).unwrap();
                let v = input.get(selected_idx).unwrap();

                op(output_idx, Some(v))
            }
        } else {
            for (output_idx, input_idx) in selection.into_iter().enumerate() {
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
        S: MutablePhysicalStorage,
        Op: FnMut(&mut S::StorageType),
    {
        let next = array.next_mut();
        let validity = &next.validity;
        let mut input = S::get_addressable_mut(next.data.try_as_mut()?)?;

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
        let flat = array.flat_view()?;

        let bools = PhysicalBool::get_addressable(flat.array_buffer)?;
        let validity = flat.validity;

        if validity.all_valid() {
            for input_idx in selection.into_iter() {
                let selected_idx = flat.selection.get(input_idx).unwrap();
                let v = *bools.get(selected_idx).unwrap();

                if v {
                    true_indices.push(input_idx);
                }
            }
        } else {
            for input_idx in selection.into_iter() {
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

    /// Get the value some index in the array.
    ///
    /// Returns Some if the value is valid, None otherwise.
    pub fn value_at2<S>(array: &Array, idx: usize) -> Result<Option<S::Type<'_>>>
    where
        S: PhysicalStorage,
    {
        let selection = array.selection_vector();

        match array.validity() {
            Some(validity) => {
                let values = S::get_storage(&array.data2)?;

                let sel = selection::get(selection, idx);
                if !validity.value(sel) {
                    Ok(None)
                } else {
                    let val = unsafe { values.get_unchecked(sel) };
                    Ok(Some(val))
                }
            }
            None => {
                let values = S::get_storage(&array.data2)?;
                let sel = selection::get(selection, idx);
                let val = unsafe { values.get_unchecked(sel) };
                Ok(Some(val))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::array_buffer::{ArrayBuffer, SecondaryBuffer};
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::physical_type::{PhysicalI32, PhysicalUtf8};
    use crate::arrays::array::string_view::{StringViewAddressableMut, StringViewHeap};
    use crate::arrays::array::validity::Validity;

    #[test]
    fn int32_inc_by_2() {
        let array = Array::try_from_iter([1, 2, 3]).unwrap();

        let mut out =
            ArrayBuffer::with_primary_capacity::<PhysicalI32>(&Arc::new(NopBufferManager), 3)
                .unwrap();
        let mut validity = Validity::new_all_valid(3);

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _>(
            &array,
            0..3,
            OutBuffer {
                buffer: &mut out,
                validity: &mut validity,
            },
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();
        assert!(validity.all_valid());

        let out_slice = out.try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(&[3, 4, 5], out_slice);
    }

    #[test]
    fn int32_inc_by_2_using_flat_view() {
        let array = Array::try_from_iter([1, 2, 3]).unwrap();

        let mut out =
            ArrayBuffer::with_primary_capacity::<PhysicalI32>(&Arc::new(NopBufferManager), 3)
                .unwrap();
        let mut validity = Validity::new_all_valid(3);

        let flat = FlatArrayView::from_array(&array).unwrap();

        UnaryExecutor::execute_flat::<PhysicalI32, PhysicalI32, _>(
            flat,
            0..3,
            OutBuffer {
                buffer: &mut out,
                validity: &mut validity,
            },
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();
        assert!(validity.all_valid());

        let out_slice = out.try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(&[3, 4, 5], out_slice);
    }

    #[test]
    fn int32_inc_by_2_in_place() {
        let mut array = Array::try_from_iter([1, 2, 3]).unwrap();

        UnaryExecutor::execute_in_place::<PhysicalI32, _>(&mut array, 0..3, |v| *v += 2).unwrap();

        let arr_slice = array.next().data.try_as_slice::<PhysicalI32>().unwrap();
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

        let mut out =
            ArrayBuffer::with_primary_capacity::<PhysicalUtf8>(&Arc::new(NopBufferManager), 6)
                .unwrap();
        out.put_secondary_buffer(SecondaryBuffer::StringViewHeap(StringViewHeap::new()));

        let mut validity = Validity::new_all_valid(6);

        fn my_string_double(s: &str, buf: PutBuffer<StringViewAddressableMut>) {
            let mut double = s.to_string();
            double.push_str(s);
            buf.put(&double);
        }

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            &array,
            0..6,
            OutBuffer {
                buffer: &mut out,
                validity: &mut validity,
            },
            my_string_double,
        )
        .unwrap();
        assert!(validity.all_valid());

        let out = out.try_as_string_view_addressable().unwrap();

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

        let mut out =
            ArrayBuffer::with_primary_capacity::<PhysicalUtf8>(&Arc::new(NopBufferManager), 6)
                .unwrap();
        out.put_secondary_buffer(SecondaryBuffer::StringViewHeap(StringViewHeap::new()));

        let mut validity = Validity::new_all_valid(6);

        let mut string_buf = String::new();

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            &array,
            0..6,
            OutBuffer {
                buffer: &mut out,
                validity: &mut validity,
            },
            |s, buf| {
                string_buf.clear();

                string_buf.push_str(s);
                string_buf.push_str(s);

                buf.put(&string_buf);
            },
        )
        .unwrap();
        assert!(validity.all_valid());

        let out = out.try_as_string_view_addressable().unwrap();

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

        let out = array.next().data.try_as_string_view_addressable().unwrap();

        assert_eq!("A", out.get(0).unwrap());
        assert_eq!("BB", out.get(1).unwrap());
        assert_eq!("CCC", out.get(2).unwrap());
    }

    #[test]
    fn int32_inc_by_2_with_dict() {
        let mut array = Array::try_from_iter([1, 2, 3]).unwrap();
        // [3, 3, 2, 1, 1, 3]
        array
            .select(&Arc::new(NopBufferManager), [2, 2, 1, 0, 0, 2])
            .unwrap();

        let mut out =
            ArrayBuffer::with_primary_capacity::<PhysicalI32>(&Arc::new(NopBufferManager), 6)
                .unwrap();
        let mut validity = Validity::new_all_valid(6);

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _>(
            &array,
            0..6,
            OutBuffer {
                buffer: &mut out,
                validity: &mut validity,
            },
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();
        assert!(validity.all_valid());

        let out_slice = out.try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(&[5, 5, 4, 3, 3, 5], out_slice);
    }
}
