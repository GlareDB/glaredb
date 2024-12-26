use rayexec_error::Result;

use super::check_validity;
use crate::arrays::array::Array;
use crate::arrays::bitmap::Bitmap;
use crate::arrays::executor::builder::{ArrayBuilder, ArrayDataBuffer, OutputBuffer};
use crate::arrays::executor::physical_type::PhysicalStorage;
use crate::arrays::executor::scalar::validate_logical_len;
use crate::arrays::selection;
use crate::arrays::storage::AddressableStorage;

#[derive(Debug, Clone, Copy)]
pub struct BinaryExecutor;

impl BinaryExecutor {
    pub fn execute<'a, S1, S2, B, Op>(
        array1: &'a Array,
        array2: &'a Array,
        builder: ArrayBuilder<B>,
        mut op: Op,
    ) -> Result<Array>
    where
        Op: FnMut(S1::Type<'a>, S2::Type<'a>, &mut OutputBuffer<B>),
        S1: PhysicalStorage,
        S2: PhysicalStorage,
        B: ArrayDataBuffer,
    {
        let len = validate_logical_len(&builder.buffer, array1)?;
        let _ = validate_logical_len(&builder.buffer, array2)?;

        let selection1 = array1.selection_vector();
        let selection2 = array2.selection_vector();

        let validity1 = array1.validity();
        let validity2 = array2.validity();

        let mut out_validity = None;

        let mut output_buffer = OutputBuffer {
            idx: 0,
            buffer: builder.buffer,
        };

        if validity1.is_some() || validity2.is_some() {
            let values1 = S1::get_storage(&array1.data)?;
            let values2 = S2::get_storage(&array2.data)?;

            let mut out_validity_builder = Bitmap::new_with_all_true(len);

            for idx in 0..len {
                let sel1 = unsafe { selection::get_unchecked(selection1, idx) };
                let sel2 = unsafe { selection::get_unchecked(selection2, idx) };

                if check_validity(sel1, validity1) && check_validity(sel2, validity2) {
                    let val1 = unsafe { values1.get_unchecked(sel1) };
                    let val2 = unsafe { values2.get_unchecked(sel2) };

                    output_buffer.idx = idx;
                    op(val1, val2, &mut output_buffer);
                } else {
                    out_validity_builder.set_unchecked(idx, false);
                }
            }

            out_validity = Some(out_validity_builder.into())
        } else {
            let values1 = S1::get_storage(&array1.data)?;
            let values2 = S2::get_storage(&array2.data)?;

            for idx in 0..len {
                let sel1 = unsafe { selection::get_unchecked(selection1, idx) };
                let sel2 = unsafe { selection::get_unchecked(selection2, idx) };

                let val1 = unsafe { values1.get_unchecked(sel1) };
                let val2 = unsafe { values2.get_unchecked(sel2) };

                output_buffer.idx = idx;
                op(val1, val2, &mut output_buffer);
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
    use crate::arrays::datatype::DataType;
    use crate::arrays::executor::builder::{GermanVarlenBuffer, PrimitiveBuffer};
    use crate::arrays::executor::physical_type::{PhysicalI32, PhysicalUtf8};
    use crate::arrays::scalar::ScalarValue;

    #[test]
    fn binary_simple_add() {
        let left = Array::from_iter([1, 2, 3]);
        let right = Array::from_iter([4, 5, 6]);

        let builder = ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::<i32>::with_len(3),
        };

        let got = BinaryExecutor::execute::<PhysicalI32, PhysicalI32, _, _>(
            &left,
            &right,
            builder,
            |a, b, buf| buf.put(&(a + b)),
        )
        .unwrap();

        assert_eq!(ScalarValue::from(5), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from(7), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from(9), got.physical_scalar(2).unwrap());
    }

    #[test]
    fn binary_string_repeat() {
        let left = Array::from_iter([1, 2, 3]);
        let right = Array::from_iter(["hello", "world", "goodbye!"]);

        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(3),
        };

        let mut string_buf = String::new();
        let got = BinaryExecutor::execute::<PhysicalI32, PhysicalUtf8, _, _>(
            &left,
            &right,
            builder,
            |repeat, s, buf| {
                string_buf.clear();
                for _ in 0..repeat {
                    string_buf.push_str(s);
                }
                buf.put(string_buf.as_str())
            },
        )
        .unwrap();

        assert_eq!(ScalarValue::from("hello"), got.physical_scalar(0).unwrap());
        assert_eq!(
            ScalarValue::from("worldworld"),
            got.physical_scalar(1).unwrap()
        );
        assert_eq!(
            ScalarValue::from("goodbye!goodbye!goodbye!"),
            got.physical_scalar(2).unwrap()
        );
    }

    #[test]
    fn binary_add_with_invalid() {
        // Make left constant null.
        let mut left = Array::from_iter([1]);
        left.put_selection(SelectionVector::repeated(3, 0));
        left.set_physical_validity(0, false);

        let right = Array::from_iter([2, 3, 4]);

        let got = BinaryExecutor::execute::<PhysicalI32, PhysicalI32, _, _>(
            &left,
            &right,
            ArrayBuilder {
                datatype: DataType::Int32,
                buffer: PrimitiveBuffer::with_len(3),
            },
            |a, b, buf| buf.put(&(a + b)),
        )
        .unwrap();

        assert_eq!(ScalarValue::Null, got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::Null, got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::Null, got.logical_value(2).unwrap());
    }
}
