use std::marker::PhantomData;

use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::datatype::DataType;
use glaredb_core::buffer::buffer_manager::{AsRawBufferManager, RawBufferManager};
use glaredb_error::{DbError, Result};

use super::read_buffer::ReadBuffer;
use super::value_reader::ValueReader;
use crate::column::encoding::Definitions;
use crate::column::encoding::plain::PlainDecoder;

#[derive(Debug)]
pub struct Dictionary<V: ValueReader> {
    manager: RawBufferManager,
    /// The array holding values from a dictionary, plus a null value.
    dictionary: Array,
    _v: PhantomData<V>,
}

impl<V> Dictionary<V>
where
    V: ValueReader,
{
    pub fn try_empty(manager: &impl AsRawBufferManager, datatype: DataType) -> Result<Self> {
        let dictionary = Array::new(manager, datatype, 0)?;
        Ok(Dictionary {
            manager: manager.as_raw_buffer_manager(),
            dictionary,
            _v: PhantomData,
        })
    }

    /// Prepares this dictionary with a new set of values.
    ///
    /// This will read `dict_size` values from the buffer, storing them in the
    /// owned dictionary array.
    pub fn prepare_with_values(&mut self, dict_size: usize, buffer: &mut ReadBuffer) -> Result<()> {
        let need_size = dict_size + 1;
        // TODO: Maybe try resizing if not shared?
        self.dictionary = Array::new(&self.manager, self.dictionary.datatype().clone(), need_size)?;

        // Read in the plain values.
        let mut decoder = PlainDecoder {
            value_reader: V::default(),
        };
        decoder.read_plain(
            buffer,
            Definitions::NoDefinitions,
            &mut self.dictionary,
            0,
            dict_size,
        )?;

        // Set last value to null.
        let validity = self.dictionary.validity_mut();
        validity.set_invalid(dict_size);

        Ok(())
    }
}
