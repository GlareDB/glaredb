use std::fmt::Debug;

use rayexec_error::Result;
use rayexec_execution::arrays::array::physical_type::{AddressableMut, MutableScalarStorage};
use rayexec_execution::arrays::array::Array;
use stdutil::marker::PhantomCovariant;

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
pub struct BasicPlainDecoder<S, V, C>
where
    S: MutableScalarStorage,
    V: ValueReader,
    C: ValueConverter<Input = V::T, Output = S::StorageType>,
{
    /// Optional dictionary buffer to read from.
    dictionary: Option<OwnedReadBuffer>,
    /// Determines how we read values from the read buffer.
    ///
    /// This may keep state (e.g. bit position when reading booleans). Read
    /// buffers should be provided in contiguous fashion to ensure the state
    /// remains consistent.
    value_reader: V,
    /// Logic for describing how to convert a value read from the buffer to a
    /// value that should be written to the array.
    _converter: PhantomCovariant<C>,
    _storage: PhantomCovariant<S>,
}

impl<S, V, C> PlainDecoder for BasicPlainDecoder<S, V, C>
where
    S: MutableScalarStorage,
    V: ValueReader,
    C: ValueConverter<Input = V::T, Output = S::StorageType>,
    S::StorageType: Sized,
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
        let mut data = S::get_addressable_mut(data)?;

        match definitions {
            Definitions::HasDefinitions { levels, max } => {
                for idx in offset..(offset + count) {
                    if levels[idx] == max {
                        // Value is null.
                        validity.set_invalid(idx);
                        continue;
                    }

                    // Value is valid, read it and put into output.
                    let v = unsafe { self.value_reader.read_unchecked(buffer) };
                    let converted = C::convert(v);
                    data.put(idx, &converted);
                }

                Ok(())
            }
            Definitions::NoDefinitions => {
                for idx in offset..(offset + count) {
                    // TODO: Just copy bytes directly if we can.

                    // Value is valid, read it and put into output.
                    let v = unsafe { self.value_reader.read_unchecked(buffer) };
                    let converted = C::convert(v);
                    data.put(idx, &converted);
                }

                Ok(())
            }
        }
    }
}
