use std::fmt::Debug;

use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::array::physical_type::{AddressableMut, MutableScalarStorage};
use glaredb_core::util::marker::PhantomCovariant;
use glaredb_error::Result;

use super::Definitions;
use crate::column::converter::ValueConverter;
use crate::column::read_buffer::{OwnedReadBuffer, ReadBuffer};
use crate::column::value_reader::ValueReader;

/// Describes decoding plain-encoded values into an output array.
pub trait PlainDecoder: Debug + Sync + Send {
    /// Reads plain-encoded values into output.
    ///
    /// The read buffer should be advanced on every value read.
    ///
    /// Definitions should be the same size as the array such that that offset
    /// into the definition matches the offset into the array.
    fn read_plain(
        &mut self,
        buffer: &mut ReadBuffer,
        definitions: Definitions,
        output: &mut Array,
        offset: usize,
        count: usize,
    ) -> Result<()>;
}

/// Reads a basic (primitive, mostly) column.
#[derive(Debug)]
pub struct BasicPlainDecoder<V>
where
    V: ValueReader,
{
    /// Optional dictionary buffer to read from.
    dictionary: Option<OwnedReadBuffer>,
    /// Determines how we read values from the read buffer.
    ///
    /// This may keep state (e.g. bit position when reading booleans). Read
    /// buffers should be provided in contiguous fashion to ensure the state
    /// remains consistent.
    value_reader: V,
}

impl<V> PlainDecoder for BasicPlainDecoder<V>
where
    V: ValueReader,
{
    fn read_plain(
        &mut self,
        buffer: &mut ReadBuffer,
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
                    if level == max {
                        // Value is null.
                        validity.set_invalid(idx);
                        continue;
                    }

                    // Value is valid, read it and put into output.
                    unsafe {
                        self.value_reader
                            .read_next_unchecked(buffer, idx, &mut data)
                    };
                }

                Ok(())
            }
            Definitions::NoDefinitions => {
                for idx in offset..(offset + count) {
                    // TODO: Just copy bytes directly if we can.

                    // Value is valid, read it and put into output.
                    unsafe {
                        self.value_reader
                            .read_next_unchecked(buffer, idx, &mut data)
                    };
                }

                Ok(())
            }
        }
    }
}
