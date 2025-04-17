use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalBinary,
    ScalarStorage,
};
use glaredb_core::buffer::buffer_manager::NopBufferManager;
use glaredb_core::buffer::typed::TypedBuffer;
use glaredb_error::{DbError, Result, ResultExt};

use super::Definitions;
use crate::column::encoding::delta_binary_packed::DeltaBinaryPackedValueDecoder;
use crate::column::read_buffer::ReadCursor;

#[derive(Debug)]
pub struct DeltaLengthByteArrayDecoder {
    /// If we should verify that the bytes we read is valid utf8.
    verify_utf8: bool,
    /// Number of values we're decoding.
    num_values: usize,
    /// Index of the current value we're on.
    curr_len_idx: usize,
    /// Decoded lengths from the beginning of the buffer.
    lengths: TypedBuffer<i32>,
    /// Cursor pointing to some byte offset.
    cursor: ReadCursor,
}

impl DeltaLengthByteArrayDecoder {
    pub fn try_new(cursor: ReadCursor, num_values: usize, verify_utf8: bool) -> Result<Self> {
        // TODO: Not Nop
        let mut lengths = TypedBuffer::<i32>::try_with_capacity(&NopBufferManager, num_values)?;

        let len_slice = &mut lengths.as_slice_mut()[..num_values]; // May overallocate
        let mut dec = DeltaBinaryPackedValueDecoder::<i32>::try_new(cursor)?;
        dec.read(len_slice)?;

        println!("LEN: {}", len_slice[0]);

        let mut cursor = dec.try_into_cursor()?;

        // WHAT IS THIS????
        unsafe { cursor.skip_bytes_unchecked(4) };

        // Verify that the total length equal the number of bytes in the cursor.
        let total = len_slice.iter().fold(0, |acc, &v| acc + v);
        if total as usize == cursor.remaining() {
            return Err(DbError::new("DELTA_LENGTH_BYTE_ARRAY: Total length does not equal remaining length in byte cursor")
                .with_field("total", total)
                .with_field("remaining", cursor.remaining()));
        }

        Ok(DeltaLengthByteArrayDecoder {
            verify_utf8,
            num_values,
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

        let lens = self.lengths.as_slice();

        match definitions {
            Definitions::HasDefinitions { levels, max } => {
                for (output_idx, &level) in levels.iter().enumerate().skip(offset).take(count) {
                    if level < max {
                        // Value is null.
                        validity.set_invalid(output_idx);
                        continue;
                    }

                    let len = lens[self.curr_len_idx];
                    println!("LEN: {len}");
                    self.curr_len_idx += 1;

                    let bs = unsafe { self.cursor.read_bytes_unchecked(len as usize) };
                    println!("BSLEN: {}", bs.len());
                    if self.verify_utf8 {
                        let s = std::str::from_utf8(bs).context("Did not read valid utf8")?;
                        println!("SLEN: {}", s.len());
                        println!("S[def]: {s}");
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
                        let s = std::str::from_utf8(bs).context("Did not read valid utf8")?;
                        println!("S: {s}");
                    }

                    data.put(output_idx, bs);
                }

                Ok(())
            }
        }
    }
}
