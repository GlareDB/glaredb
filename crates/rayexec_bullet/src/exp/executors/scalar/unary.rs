use rayexec_error::Result;

use crate::compute::util::IntoExactSizedIterator;
use crate::exp::array::{Array, DictionaryArrayView};
use crate::exp::buffer::addressable::{AddressableStorage, MutableAddressableStorage};
use crate::exp::buffer::physical_type::{MutablePhysicalStorage, PhysicalStorage};
use crate::exp::buffer::ArrayBuffer;
use crate::exp::executors::OutputBuffer;
use crate::exp::validity::Validity;

#[derive(Debug, Clone)]
pub struct UnaryExecutor;

impl UnaryExecutor {
    /// Execute a unary operation on `array`, placing results in `out`.
    pub fn execute<S, O, Op>(
        array: &Array,
        selection: impl IntoExactSizedIterator<Item = usize>,
        out: &mut ArrayBuffer,
        out_validity: &mut Validity,
        mut op: Op,
    ) -> Result<()>
    where
        S: PhysicalStorage,
        O: MutablePhysicalStorage,
        for<'a> Op: FnMut(&S::StorageType, OutputBuffer<O::MutableStorage<'a>>),
    {
        if array.is_dictionary() {
            let view = DictionaryArrayView::try_from_array(array)?;
            return Self::execute_dictionary::<S, _, _>(view, selection, out, out_validity, op);
        }

        let input = S::get_storage(array.buffer())?;
        let mut output = O::get_storage_mut(out)?;

        let validity = array.validity();

        if validity.all_valid() {
            for (output_idx, input_idx) in selection.into_iter().enumerate() {
                op(
                    input.get(input_idx).unwrap(),
                    OutputBuffer::new(output_idx, &mut output, out_validity),
                );
            }
        } else {
            for (output_idx, input_idx) in selection.into_iter().enumerate() {
                if validity.is_valid(input_idx) {
                    op(
                        input.get(input_idx).unwrap(),
                        OutputBuffer::new(output_idx, &mut output, out_validity),
                    );
                } else {
                    out_validity.set_invalid(output_idx);
                }
            }
        }

        Ok(())
    }

    pub fn execute_dictionary<'a, S, O, Op>(
        array: DictionaryArrayView<'a>,
        selection: impl IntoExactSizedIterator<Item = usize>,
        out: &mut ArrayBuffer,
        out_validity: &mut Validity,
        mut op: Op,
    ) -> Result<()>
    where
        S: PhysicalStorage,
        O: MutablePhysicalStorage,
        for<'b> Op: FnMut(&S::StorageType, OutputBuffer<O::MutableStorage<'b>>),
    {
        let input = S::get_storage(&array.array_buffer)?;
        let mut output = O::get_storage_mut(out)?;

        let validity = array.validity;

        if validity.all_valid() {
            for (output_idx, input_idx) in selection.into_iter().enumerate() {
                let selected_idx = array.selection[input_idx];

                op(
                    input.get(selected_idx).unwrap(),
                    OutputBuffer::new(output_idx, &mut output, out_validity),
                );
            }
        } else {
            for (output_idx, input_idx) in selection.into_iter().enumerate() {
                let selected_idx = array.selection[input_idx];

                if validity.is_valid(selected_idx) {
                    op(
                        input.get(selected_idx).unwrap(),
                        OutputBuffer::new(output_idx, &mut output, out_validity),
                    );
                } else {
                    out_validity.set_invalid(output_idx);
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
    pub fn execute_in_place<S, Op>(array: &mut Array, mut op: Op) -> Result<()>
    where
        S: MutablePhysicalStorage,
        Op: FnMut(&mut S::StorageType),
    {
        let validity = &array.validity;
        let mut input = S::get_storage_mut(&mut array.buffer)?;

        if validity.all_valid() {
            for idx in 0..input.len() {
                op(input.get_mut(idx).unwrap());
            }
        } else {
            for idx in 0..input.len() {
                if validity.is_valid(idx) {
                    op(input.get_mut(idx).unwrap());
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatype::DataType;
    use crate::exp::buffer::physical_type::{PhysicalI32, PhysicalUtf8};
    use crate::exp::buffer::reservation::NopReservationTracker;
    use crate::exp::buffer::string_view::{StringViewHeap, StringViewStorageMut};
    use crate::exp::buffer::{Int32Builder, StringViewBufferBuilder};

    #[test]
    fn int32_inc_by_2() {
        let array = Array::new(DataType::Int32, Int32Builder::from_iter([1, 2, 3]).unwrap());
        let mut out = ArrayBuffer::with_len::<PhysicalI32>(&NopReservationTracker, 3).unwrap();
        let mut validity = Validity::new_all_valid(3);

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _>(
            &array,
            0..3,
            &mut out,
            &mut validity,
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();
        assert!(validity.all_valid());

        let out_slice = out.try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(&[3, 4, 5], out_slice);
    }

    #[test]
    fn int32_inc_by_2_in_place() {
        let mut array = Array::new(DataType::Int32, Int32Builder::from_iter([1, 2, 3]).unwrap());

        UnaryExecutor::execute_in_place::<PhysicalI32, _>(&mut array, |v| *v = *v + 2).unwrap();

        let arr_slice = array.buffer().try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(&[3, 4, 5], arr_slice);
    }

    #[test]
    fn string_double_named_func() {
        // Example with defined function, and allocating a new string every time.
        let array = Array::new(
            DataType::Utf8,
            StringViewBufferBuilder::from_iter([
                "a",
                "bb",
                "ccc",
                "dddd",
                "heapafter", // Inlined, will be moved to heap after doubling.
                "alongerstringdontinline",
            ])
            .unwrap(),
        );

        let mut out = ArrayBuffer::with_len_and_child_buffer::<PhysicalUtf8>(
            &NopReservationTracker,
            6,
            StringViewHeap::new(),
        )
        .unwrap();
        let mut validity = Validity::new_all_valid(6);

        fn my_string_double(s: &str, buf: OutputBuffer<StringViewStorageMut>) {
            let mut double = s.to_string();
            double.push_str(s);
            buf.put(&double);
        }

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            &array,
            0..6,
            &mut out,
            &mut validity,
            my_string_double,
        )
        .unwrap();
        assert!(validity.all_valid());

        let out = out.try_as_string_view_storage().unwrap();

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
        let array = Array::new(
            DataType::Utf8,
            StringViewBufferBuilder::from_iter([
                "a",
                "bb",
                "ccc",
                "dddd",
                "heapafter", // Inlined, will be moved to heap after doubling.
                "alongerstringdontinline",
            ])
            .unwrap(),
        );

        let mut out = ArrayBuffer::with_len_and_child_buffer::<PhysicalUtf8>(
            &NopReservationTracker,
            6,
            StringViewHeap::new(),
        )
        .unwrap();
        let mut validity = Validity::new_all_valid(6);

        let mut string_buf = String::new();

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            &array,
            0..6,
            &mut out,
            &mut validity,
            |s, buf| {
                string_buf.clear();

                string_buf.push_str(s);
                string_buf.push_str(s);

                buf.put(&string_buf);
            },
        )
        .unwrap();
        assert!(validity.all_valid());

        let out = out.try_as_string_view_storage().unwrap();

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
        let mut array = Array::new(
            DataType::Utf8,
            StringViewBufferBuilder::from_iter(["a", "bb", "ccc"]).unwrap(),
        );

        UnaryExecutor::execute_in_place::<PhysicalUtf8, _>(&mut array, |v| {
            v.make_ascii_uppercase()
        })
        .unwrap();

        let out = array.buffer().try_as_string_view_storage().unwrap();

        assert_eq!("A", out.get(0).unwrap());
        assert_eq!("BB", out.get(1).unwrap());
        assert_eq!("CCC", out.get(2).unwrap());
    }

    #[test]
    fn int32_inc_by_2_with_dict() {
        let mut array = Array::new(DataType::Int32, Int32Builder::from_iter([1, 2, 3]).unwrap());
        // [3, 3, 2, 1, 1, 3]
        array
            .select(&NopReservationTracker, [2, 2, 1, 0, 0, 2])
            .unwrap();

        let mut out = ArrayBuffer::with_len::<PhysicalI32>(&NopReservationTracker, 6).unwrap();
        let mut validity = Validity::new_all_valid(6);

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _>(
            &array,
            0..6,
            &mut out,
            &mut validity,
            |&v, buf| buf.put(&(v + 2)),
        )
        .unwrap();
        assert!(validity.all_valid());

        let out_slice = out.try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(&[5, 5, 4, 3, 3, 5], out_slice);
    }
}
