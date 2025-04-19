use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalBinary,
};
use glaredb_core::buffer::buffer_manager::DefaultBufferManager;
use glaredb_core::buffer::db_vec::DbVec;
use glaredb_error::{DbError, Result, ResultExt};

use super::Definitions;
use crate::column::encoding::delta_binary_packed::DeltaBinaryPackedValueDecoder;
use crate::column::read_buffer::ReadCursor;

#[derive(Debug)]
pub struct DeltaLengthByteArrayDecoder {
    /// If we should verify that the bytes we read is valid utf8.
    verify_utf8: bool,
    /// Index of the current value we're on.
    curr_len_idx: usize,
    /// Decoded lengths from the beginning of the buffer.
    lengths: DbVec<i32>,
    /// Cursor pointing to some byte offset.
    cursor: ReadCursor,
}

impl DeltaLengthByteArrayDecoder {
    pub fn try_new(cursor: ReadCursor, verify_utf8: bool) -> Result<Self> {
        let mut dec = DeltaBinaryPackedValueDecoder::<i32>::try_new(cursor)?;
        let num_values = dec.total_values();

        // TODO: Not Nop
        let mut lengths = DbVec::<i32>::new_uninit(&DefaultBufferManager, num_values)?;
        let len_slice = unsafe { lengths.as_slice_mut() };
        dec.read(len_slice)?;

        let cursor = dec.try_into_cursor()?;

        // Verify that the total length equal the number of bytes in the cursor.
        let total: i32 = len_slice.iter().sum();
        if total as usize != cursor.remaining() {
            return Err(DbError::new("DELTA_LENGTH_BYTE_ARRAY: Total length does not equal remaining length in byte cursor")
                .with_field("total", total)
                .with_field("remaining", cursor.remaining()));
        }

        Ok(DeltaLengthByteArrayDecoder {
            verify_utf8,
            curr_len_idx: 0,
            lengths,
            cursor,
        })
    }

    pub fn read(
        &mut self,
        definitions: Definitions,
        output: &mut Array,
        offset: usize,
        count: usize,
    ) -> Result<()> {
        let (data, validity) = output.data_and_validity_mut();
        let mut data = PhysicalBinary::get_addressable_mut(data)?;

        let lens = unsafe { self.lengths.as_slice() };

        match definitions {
            Definitions::HasDefinitions { levels, max } => {
                for (output_idx, &level) in levels.iter().enumerate().skip(offset).take(count) {
                    if level < max {
                        // Value is null.
                        validity.set_invalid(output_idx);
                        continue;
                    }

                    let len = lens[self.curr_len_idx];
                    self.curr_len_idx += 1;

                    let bs = unsafe { self.cursor.read_bytes_unchecked(len as usize) };
                    if self.verify_utf8 {
                        let _ = std::str::from_utf8(bs).context("Did not read valid utf8")?;
                    }

                    data.put(output_idx, bs);
                }

                Ok(())
            }
            Definitions::NoDefinitions => {
                for output_idx in offset..(offset + count) {
                    let len = lens[self.curr_len_idx];
                    self.curr_len_idx += 1;

                    let bs = unsafe { self.cursor.read_bytes_unchecked(len as usize) };
                    if self.verify_utf8 {
                        let _ = std::str::from_utf8(bs).context("Did not read valid utf8")?;
                    }

                    data.put(output_idx, bs);
                }

                Ok(())
            }
        }
    }
}
