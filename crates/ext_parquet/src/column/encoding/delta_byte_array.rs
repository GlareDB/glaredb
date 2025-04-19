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
pub struct DeltaByteArrayDecoder {
    /// If we should verify that the bytes we read is valid utf8.
    verify_utf8: bool,
    /// Most recent value we've read.
    ///
    /// For each index, we truncate this to the prefix len, then extend with the
    /// next 'suffix' bytes.
    ///
    /// Vec since that has a nicer API for this.
    val_buf: Vec<u8>,
    /// Index of the current prefix/suffix length we're on.
    curr_len_idx: usize,
    /// Decoded prefix lengths.
    prefix_lengths: DbVec<i32>,
    /// Decoded suffix lengths.
    suffix_lengths: DbVec<i32>,
    /// Cursor pointing to the current byte offset.
    cursor: ReadCursor,
}

impl DeltaByteArrayDecoder {
    pub fn try_new(cursor: ReadCursor, verify_utf8: bool) -> Result<Self> {
        // Read delta binary packed lengths.
        let read_lengths = |cursor| -> Result<(_, _, _)> {
            // TODO: Not Nop
            let mut dec = DeltaBinaryPackedValueDecoder::<i32>::try_new(cursor)?;
            let num_values = dec.total_values();

            let mut lengths = DbVec::<i32>::new_uninit(&DefaultBufferManager, num_values)?;
            let len_slice = unsafe { lengths.as_slice_mut() };
            dec.read(len_slice)?;

            let cursor = dec.try_into_cursor()?;

            Ok((lengths, cursor, num_values))
        };

        let (prefix_lengths, cursor, prefix_count) = read_lengths(cursor)?;
        let (suffix_lengths, cursor, suffix_count) = read_lengths(cursor)?;

        if prefix_count != suffix_count {
            return Err(
                DbError::new("Decoded different number of prefix and suffix lengths")
                    .with_field("prefix", prefix_count)
                    .with_field("suffix", suffix_count),
            );
        }

        Ok(DeltaByteArrayDecoder {
            verify_utf8,
            val_buf: Vec::new(),
            curr_len_idx: 0,
            prefix_lengths,
            suffix_lengths,
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

        let prefix_lens = unsafe { self.prefix_lengths.as_slice() };
        let suffix_lens = unsafe { self.suffix_lengths.as_slice() };

        match definitions {
            Definitions::HasDefinitions { levels, max } => {
                for (output_idx, &level) in levels.iter().enumerate().skip(offset).take(count) {
                    if level < max {
                        // Value is null.
                        validity.set_invalid(output_idx);
                        continue;
                    }

                    let prefix = prefix_lens[self.curr_len_idx] as usize;
                    let suffix = suffix_lens[self.curr_len_idx] as usize;
                    self.curr_len_idx += 1;

                    self.val_buf.truncate(prefix);
                    if self.cursor.remaining() < suffix {
                        return Err(DbError::new("Not enough bytes remaining in the cursor"));
                    }
                    let bs = unsafe { self.cursor.read_bytes_unchecked(suffix) };
                    self.val_buf.extend_from_slice(bs);

                    if self.verify_utf8 {
                        let _ = std::str::from_utf8(&self.val_buf)
                            .context("Did not read valid utf8")?;
                    }

                    data.put(output_idx, &self.val_buf);
                }

                Ok(())
            }
            Definitions::NoDefinitions => {
                for output_idx in offset..(offset + count) {
                    let prefix = prefix_lens[self.curr_len_idx] as usize;
                    let suffix = suffix_lens[self.curr_len_idx] as usize;
                    self.curr_len_idx += 1;

                    self.val_buf.truncate(prefix);
                    if self.cursor.remaining() < suffix {
                        return Err(DbError::new("Not enough bytes remaining in the cursor"));
                    }
                    let bs = unsafe { self.cursor.read_bytes_unchecked(suffix) };
                    self.val_buf.extend_from_slice(bs);

                    if self.verify_utf8 {
                        let _ = std::str::from_utf8(&self.val_buf)
                            .context("Did not read valid utf8")?;
                    }

                    data.put(output_idx, &self.val_buf);
                }

                Ok(())
            }
        }
    }
}
