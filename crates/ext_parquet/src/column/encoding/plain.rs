use std::fmt::Debug;

use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::array::physical_type::MutableScalarStorage;
use glaredb_error::Result;

use super::Definitions;
use crate::column::read_buffer::ReadCursor;
use crate::column::value_reader::ValueReader;

/// Decodes plain-encoded values into an output array.
#[derive(Debug)]
pub struct PlainDecoder<V>
where
    V: ValueReader,
{
    /// Buffer we're reading from.
    pub buffer: ReadCursor,
    /// Determines how we read values from the read buffer.
    ///
    /// This may keep state (e.g. bit position when reading booleans). Read
    /// buffers should be provided in contiguous fashion to ensure the state
    /// remains consistent.
    pub value_reader: V,
}

impl<V> PlainDecoder<V>
where
    V: ValueReader,
{
    /// Reads plain-encoded values into output.
    ///
    /// The read buffer should be advanced on every value read.
    ///
    /// Definitions should be the same size as the array such that that offset
    /// into the definition matches the offset into the array.
    pub fn read_plain(
        &mut self,
        definitions: Definitions,
        output: &mut Array,
        offset: usize,
        count: usize,
    ) -> Result<()> {
        let (data, validity) = output.data_and_validity_mut();
        let mut data = <V::Storage>::get_addressable_mut(data)?;

        match definitions {
            Definitions::HasDefinitions { levels, max } => {
                for (idx, &level) in levels.iter().enumerate().skip(offset).take(count) {
                    if level < max {
                        // Value is null.
                        validity.set_invalid(idx);
                        continue;
                    }

                    // Value is valid, read it and put into output.
                    unsafe {
                        self.value_reader
                            .read_next_unchecked(&mut self.buffer, idx, &mut data)
                    };
                }

                Ok(())
            }
            Definitions::NoDefinitions => {
                for idx in offset..(offset + count) {
                    // Value is valid, read it and put into output.
                    unsafe {
                        self.value_reader
                            .read_next_unchecked(&mut self.buffer, idx, &mut data)
                    };
                }

                Ok(())
            }
        }
    }
}
