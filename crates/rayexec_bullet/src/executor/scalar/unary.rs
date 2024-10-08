use crate::{
    array::Array,
    bitmap::Bitmap,
    executor::{
        builder::{ArrayBuilder, ArrayDataBuffer, OutputBuffer},
        physical_type::PhysicalStorage,
    },
    selection,
    storage::AddressableStorage,
};
use rayexec_error::Result;

use super::validate_logical_len;

#[derive(Debug, Clone)]
pub struct UnaryExecutor;

impl UnaryExecutor {
    pub fn execute<'a, S, B, Op>(
        array: &'a Array,
        builder: ArrayBuilder<B>,
        mut op: Op,
    ) -> Result<Array>
    where
        Op: FnMut(<S::Storage as AddressableStorage>::T, &mut OutputBuffer<B>),
        S: PhysicalStorage<'a>,
        B: ArrayDataBuffer,
    {
        let len = validate_logical_len(&builder.buffer, array)?;

        let selection = array.selection_vector();
        let mut out_validity = None;

        let mut output_buffer = OutputBuffer {
            idx: 0,
            buffer: builder.buffer,
        };

        match &array.validity {
            Some(validity) => {
                let values = S::get_storage(&array.data)?;
                let mut out_validity_builder = Bitmap::new_with_all_true(len);

                for idx in 0..len {
                    let sel = selection::get_unchecked(selection, idx);
                    if !validity.value_unchecked(sel) {
                        out_validity_builder.set_unchecked(idx, false);
                        continue;
                    }

                    let val = unsafe { values.get_unchecked(sel) };

                    output_buffer.idx = idx;
                    op(val, &mut output_buffer);
                }

                out_validity = Some(out_validity_builder)
            }
            None => {
                let values = S::get_storage(&array.data)?;
                for idx in 0..len {
                    let sel = selection::get_unchecked(selection, idx);
                    let val = unsafe { values.get_unchecked(sel) };

                    output_buffer.idx = idx;
                    op(val, &mut output_buffer);
                }
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

    /// Helper for iterating over all values in an array, taking into account
    /// the array's selection vector and validity mask.
    ///
    /// `op` is called for each logical entry in the array with the index and
    /// either Some(val) if the value is valid, or None if it's not.
    pub fn for_each<'a, S, Op>(array: &'a Array, mut op: Op) -> Result<()>
    where
        Op: FnMut(usize, Option<<S::Storage as AddressableStorage>::T>),
        S: PhysicalStorage<'a>,
    {
        let selection = array.selection_vector();
        let len = array.logical_len();

        match &array.validity {
            Some(validity) => {
                let values = S::get_storage(&array.data)?;

                for idx in 0..len {
                    let sel = selection::get_unchecked(selection, idx);
                    if !validity.value_unchecked(sel) {
                        op(idx, None);
                        continue;
                    }

                    let val = unsafe { values.get_unchecked(sel) };
                    op(idx, Some(val));
                }
            }
            None => {
                let values = S::get_storage(&array.data)?;
                for idx in 0..len {
                    let sel = selection::get_unchecked(selection, idx);
                    let val = unsafe { values.get_unchecked(sel) };
                    op(idx, Some(val));
                }
            }
        }

        Ok(())
    }

    pub fn value_at_unchecked<'a, S>(
        array: &'a Array,
        idx: usize,
    ) -> Result<Option<<S::Storage as AddressableStorage>::T>>
    where
        S: PhysicalStorage<'a>,
    {
        let selection = array.selection_vector();

        match &array.validity {
            Some(validity) => {
                let values = S::get_storage(&array.data)?;

                let sel = selection::get_unchecked(selection, idx);
                if !validity.value_unchecked(sel) {
                    Ok(None)
                } else {
                    let val = unsafe { values.get_unchecked(sel) };
                    Ok(Some(val))
                }
            }
            None => {
                let values = S::get_storage(&array.data)?;
                let sel = selection::get_unchecked(selection, idx);
                let val = unsafe { values.get_unchecked(sel) };
                Ok(Some(val))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use selection::SelectionVector;

    use crate::{
        datatype::DataType,
        executor::{
            builder::{GermanVarlenBuffer, PrimitiveBuffer},
            physical_type::{PhysicalI32, PhysicalUtf8},
        },
        scalar::ScalarValue,
    };

    use super::*;

    #[test]
    fn int32_inc_by_2() {
        let array = Array::from_iter([1, 2, 3]);
        let builder = ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::<i32>::with_len(3),
        };

        let got = UnaryExecutor::execute::<PhysicalI32, _, _>(&array, builder, |v, buf| {
            buf.put(&(v + 2))
        })
        .unwrap();

        assert_eq!(ScalarValue::from(3), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from(4), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from(5), got.physical_scalar(2).unwrap());
    }

    #[test]
    fn string_double_named_func() {
        // Example with defined function, and allocating a new string every time.

        let array = Array::from_iter(["a", "bb", "ccc", "dddd"]);
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(4),
        };

        fn my_string_double<'a, B>(s: &str, buf: &mut OutputBuffer<B>)
        where
            B: ArrayDataBuffer<Type = str>,
        {
            let mut double = s.to_string();
            double.push_str(s);
            buf.put(&double)
        }

        let got = UnaryExecutor::execute::<PhysicalUtf8, _, _>(&array, builder, my_string_double)
            .unwrap();

        assert_eq!(ScalarValue::from("aa"), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from("bbbb"), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from("cccccc"), got.physical_scalar(2).unwrap());
        assert_eq!(
            ScalarValue::from("dddddddd"),
            got.physical_scalar(3).unwrap()
        );
    }

    #[test]
    fn string_double_closure() {
        // Example with closure that reuses a string.

        let array = Array::from_iter(["a", "bb", "ccc", "dddd"]);
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(4),
        };

        let mut buffer = String::new();

        let my_string_double = |s: &str, buf: &mut OutputBuffer<_>| {
            buffer.clear();

            buffer.push_str(s);
            buffer.push_str(s);

            buf.put(buffer.as_str())
        };

        let got = UnaryExecutor::execute::<PhysicalUtf8, _, _>(&array, builder, my_string_double)
            .unwrap();

        assert_eq!(ScalarValue::from("aa"), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from("bbbb"), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from("cccccc"), got.physical_scalar(2).unwrap());
        assert_eq!(
            ScalarValue::from("dddddddd"),
            got.physical_scalar(3).unwrap()
        );
    }

    #[test]
    fn string_trunc_closure() {
        // Example with closure returning referencing to input.

        let array = Array::from_iter(["a", "bb", "ccc", "dddd"]);
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(4),
        };

        let my_string_truncate = |s: &str, buf: &mut OutputBuffer<_>| {
            let len = std::cmp::min(2, s.len());
            buf.put(s.get(0..len).unwrap_or(""))
        };

        let got = UnaryExecutor::execute::<PhysicalUtf8, _, _>(&array, builder, my_string_truncate)
            .unwrap();

        assert_eq!(ScalarValue::from("a"), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from("bb"), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from("cc"), got.physical_scalar(2).unwrap());
        assert_eq!(ScalarValue::from("dd"), got.physical_scalar(3).unwrap());
    }

    #[test]
    fn string_select_uppercase() {
        // Example with selection vector whose logical length is greater than
        // the underlying physical data len.

        let mut array = Array::from_iter(["a", "bb", "ccc", "dddd"]);
        let mut selection = SelectionVector::with_range(0..5);
        selection.set_unchecked(0, 3);
        selection.set_unchecked(1, 3);
        selection.set_unchecked(2, 3);
        selection.set_unchecked(3, 1);
        selection.set_unchecked(4, 2);
        array.select_mut(&selection.into());

        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(array.logical_len()),
        };

        let my_string_uppercase = |s: &str, buf: &mut OutputBuffer<_>| {
            let s = s.to_uppercase();
            buf.put(s.as_str())
        };

        let got =
            UnaryExecutor::execute::<PhysicalUtf8, _, _>(&array, builder, my_string_uppercase)
                .unwrap();

        assert_eq!(ScalarValue::from("DDDD"), got.physical_scalar(0).unwrap());
        assert_eq!(ScalarValue::from("DDDD"), got.physical_scalar(1).unwrap());
        assert_eq!(ScalarValue::from("DDDD"), got.physical_scalar(2).unwrap());
        assert_eq!(ScalarValue::from("BB"), got.physical_scalar(3).unwrap());
        assert_eq!(ScalarValue::from("CCC"), got.physical_scalar(4).unwrap());
    }
}
