use std::fmt::Debug;

use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::array::physical_type::MutableScalarStorage;
use glaredb_error::Result;

use super::Definitions;
use crate::column::read_buffer::ReadCursor;
use crate::column::value_reader::{ReaderErrorState, ValueReader};

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
                debug_assert_eq!(levels.len(), count);

                let mut error_state = ReaderErrorState::default();
                for (idx, &level) in levels.iter().enumerate() {
                    let write_idx = offset + idx;
                    if level < max {
                        // Value is null.
                        validity.set_invalid(write_idx);
                        continue;
                    }

                    // Value is valid, read it and put into output.
                    unsafe {
                        self.value_reader.read_next_unchecked(
                            &mut self.buffer,
                            write_idx,
                            &mut data,
                            &mut error_state,
                        )
                    };
                }

                error_state.into_result()
            }
            Definitions::NoDefinitions => {
                let mut error_state = ReaderErrorState::default();
                for idx in 0..count {
                    let write_idx = idx + offset;
                    // Value is valid, read it and put into output.
                    unsafe {
                        self.value_reader.read_next_unchecked(
                            &mut self.buffer,
                            write_idx,
                            &mut data,
                            &mut error_state,
                        )
                    };
                }

                error_state.into_result()
            }
        }
    }
}
