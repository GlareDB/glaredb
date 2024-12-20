use rayexec_error::Result;

use super::OutputBuffer;
use crate::compute::util::IntoExactSizedIterator;
use crate::exp::array::Array;
use crate::exp::buffer::addressable::AddressableStorage;
use crate::exp::buffer::physical_type::{MutablePhysicalStorage, PhysicalStorage};
use crate::exp::buffer::ArrayBuffer;
use crate::exp::validity::Validity;

#[derive(Debug, Clone)]
pub struct BinaryExecutor;

impl BinaryExecutor {
    pub fn execute<S1, S2, O, Op>(
        array1: &Array,
        sel1: impl IntoExactSizedIterator<Item = usize>,
        array2: &Array,
        sel2: impl IntoExactSizedIterator<Item = usize>,
        out: &mut ArrayBuffer,
        out_validity: &mut Validity,
        mut op: Op,
    ) -> Result<()>
    where
        S1: PhysicalStorage,
        S2: PhysicalStorage,
        O: MutablePhysicalStorage,
        for<'a> Op: FnMut(&S1::StorageType, &S2::StorageType, OutputBuffer<O::MutableStorage<'a>>),
    {
        // TODO: length validation

        let input1 = S1::get_storage(array1.buffer())?;
        let input2 = S2::get_storage(array2.buffer())?;

        let mut output = O::get_storage_mut(out)?;

        let validity1 = array1.validity();
        let validity2 = array2.validity();

        if validity1.all_valid() && validity2.all_valid() {
            for (output_idx, (input1_idx, input2_idx)) in
                sel1.into_iter().zip(sel2.into_iter()).enumerate()
            {
                let val1 = input1.get(input1_idx).unwrap();
                let val2 = input2.get(input2_idx).unwrap();

                op(
                    val1,
                    val2,
                    OutputBuffer {
                        idx: output_idx,
                        buffer: &mut output,
                    },
                );
            }
        } else {
            for (output_idx, (input1_idx, input2_idx)) in
                sel1.into_iter().zip(sel2.into_iter()).enumerate()
            {
                if validity1.is_valid(input1_idx) && validity2.is_valid(input2_idx) {
                    let val1 = input1.get(input1_idx).unwrap();
                    let val2 = input2.get(input2_idx).unwrap();

                    op(
                        val1,
                        val2,
                        OutputBuffer {
                            idx: output_idx,
                            buffer: &mut output,
                        },
                    );
                } else {
                    out_validity.set_invalid(output_idx);
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
    use crate::exp::buffer::string_view::StringViewHeap;
    use crate::exp::buffer::{Int32Builder, StringViewBufferBuilder};

    #[test]
    fn binary_simple_add() {
        let left = Array::new(DataType::Int32, Int32Builder::from_iter([1, 2, 3]).unwrap());
        let right = Array::new(DataType::Int32, Int32Builder::from_iter([4, 5, 6]).unwrap());

        let mut out = ArrayBuffer::with_len::<PhysicalI32>(&NopReservationTracker, 3).unwrap();
        let mut validity = Validity::new_all_valid(3);

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _>(
            &left,
            0..3,
            &right,
            0..3,
            &mut out,
            &mut validity,
            |&a, &b, buf| buf.put(&(a + b)),
        )
        .unwrap();
        assert!(validity.all_valid());

        let out_slice = out.try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(&[5, 7, 9], out_slice);
    }

    #[test]
    fn binary_string_repeat() {
        let left = Array::new(DataType::Int32, Int32Builder::from_iter([1, 2, 3]).unwrap());
        let right = Array::new(
            DataType::Utf8,
            StringViewBufferBuilder::from_iter(["hello", "world", "goodbye!"]).unwrap(),
        );

        let mut out = ArrayBuffer::with_len_and_child_buffer::<PhysicalUtf8>(
            &NopReservationTracker,
            3,
            StringViewHeap::new(),
        )
        .unwrap();
        let mut validity = Validity::new_all_valid(3);

        let mut string_buf = String::new();
        BinaryExecutor::execute::<PhysicalI32, PhysicalUtf8, PhysicalUtf8, _>(
            &left,
            0..3,
            &right,
            0..3,
            &mut out,
            &mut validity,
            |&repeat, s, buf| {
                string_buf.clear();
                for _ in 0..repeat {
                    string_buf.push_str(s);
                }
                buf.put(&string_buf);
            },
        )
        .unwrap();
        assert!(validity.all_valid());

        let out = out.try_as_string_view_storage().unwrap();
        assert_eq!("hello", out.get(0).unwrap());
        assert_eq!("worldworld", out.get(1).unwrap());
        assert_eq!("goodbye!goodbye!goodbye!", out.get(2).unwrap());
    }

    #[test]
    fn binary_add_with_invalid() {
        let mut left_validity = Validity::new_all_valid(3);
        left_validity.set_invalid(1);
        let left = Array::new_with_validity(
            DataType::Int32,
            Int32Builder::from_iter([1, 2, 3]).unwrap(),
            left_validity,
        )
        .unwrap();

        let right = Array::new(DataType::Int32, Int32Builder::from_iter([4, 5, 6]).unwrap());

        let mut out = ArrayBuffer::with_len::<PhysicalI32>(&NopReservationTracker, 3).unwrap();
        let mut validity = Validity::new_all_valid(3);

        BinaryExecutor::execute::<PhysicalI32, PhysicalI32, PhysicalI32, _>(
            &left,
            0..3,
            &right,
            0..3,
            &mut out,
            &mut validity,
            |&a, &b, buf| buf.put(&(a + b)),
        )
        .unwrap();

        let out_slice = out.try_as_slice::<PhysicalI32>().unwrap();

        assert!(validity.is_valid(0));
        assert_eq!(5, out_slice[0]);

        assert!(!validity.is_valid(1));

        assert!(validity.is_valid(2));
        assert_eq!(9, out_slice[2]);
    }
}
