use std::fmt::Debug;

use rayexec_error::Result;

use super::check_validity;
use crate::array::Array;
use crate::bitmap::Bitmap;
use crate::executor::builder::{ArrayBuilder, ArrayDataBuffer, OutputBuffer};
use crate::executor::physical_type::PhysicalStorage;
use crate::executor::scalar::validate_logical_len;
use crate::selection;
use crate::storage::AddressableStorage;

#[derive(Debug, Clone, Copy)]
pub struct TernaryExecutor;

impl TernaryExecutor {
    pub fn execute<'a, S1, S2, S3, B, Op>(
        array1: &'a Array,
        array2: &'a Array,
        array3: &'a Array,
        builder: ArrayBuilder<B>,
        mut op: Op,
    ) -> Result<Array>
    where
        Op: FnMut(
            <S1::Storage as AddressableStorage>::T,
            <S2::Storage as AddressableStorage>::T,
            <S3::Storage as AddressableStorage>::T,
            &mut OutputBuffer<B>,
        ),
        S1: PhysicalStorage<'a>,
        S2: PhysicalStorage<'a>,
        S3: PhysicalStorage<'a>,
        B: ArrayDataBuffer,
    {
        let len = validate_logical_len(&builder.buffer, array1)?;
        let _ = validate_logical_len(&builder.buffer, array2)?;
        let _ = validate_logical_len(&builder.buffer, array3)?;

        let selection1 = array1.selection_vector();
        let selection2 = array2.selection_vector();
        let selection3 = array3.selection_vector();

        let validity1 = array1.validity();
        let validity2 = array2.validity();
        let validity3 = array3.validity();

        let mut out_validity = None;

        let mut output_buffer = OutputBuffer {
            idx: 0,
            buffer: builder.buffer,
        };

        if validity1.is_some() || validity2.is_some() || validity3.is_some() {
            let values1 = S1::get_storage(&array1.data)?;
            let values2 = S2::get_storage(&array2.data)?;
            let values3 = S3::get_storage(&array3.data)?;

            let mut out_validity_builder = Bitmap::new_with_all_true(len);

            for idx in 0..len {
                let sel1 = selection::get_unchecked(selection1, idx);
                let sel2 = selection::get_unchecked(selection2, idx);
                let sel3 = selection::get_unchecked(selection3, idx);

                if check_validity(sel1, validity1)
                    && check_validity(sel2, validity2)
                    && check_validity(sel3, validity3)
                {
                    let val1 = unsafe { values1.get_unchecked(sel1) };
                    let val2 = unsafe { values2.get_unchecked(sel2) };
                    let val3 = unsafe { values3.get_unchecked(sel3) };

                    output_buffer.idx = idx;
                    op(val1, val2, val3, &mut output_buffer);
                } else {
                    out_validity_builder.set_unchecked(idx, false);
                }
            }

            out_validity = Some(out_validity_builder)
        } else {
            let values1 = S1::get_storage(&array1.data)?;
            let values2 = S2::get_storage(&array2.data)?;
            let values3 = S3::get_storage(&array3.data)?;

            for idx in 0..len {
                let sel1 = selection::get_unchecked(selection1, idx);
                let sel2 = selection::get_unchecked(selection2, idx);
                let sel3 = selection::get_unchecked(selection3, idx);

                let val1 = unsafe { values1.get_unchecked(sel1) };
                let val2 = unsafe { values2.get_unchecked(sel2) };
                let val3 = unsafe { values3.get_unchecked(sel3) };

                output_buffer.idx = idx;
                op(val1, val2, val3, &mut output_buffer);
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
