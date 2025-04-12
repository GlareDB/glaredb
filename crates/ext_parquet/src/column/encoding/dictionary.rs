use std::marker::PhantomData;

use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::compute;
use glaredb_core::arrays::compute::copy::copy_rows_array;
use glaredb_core::arrays::datatype::DataType;
use glaredb_core::buffer::buffer_manager::{AsRawBufferManager, RawBufferManager};
use glaredb_error::{Result, not_implemented};

use super::rle_bp::RleBpDecoder;
use crate::column::encoding::Definitions;
use crate::column::encoding::plain::PlainDecoder;
use crate::column::read_buffer::ReadCursor;
use crate::column::value_reader::ValueReader;

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
    pub fn prepare_with_values(&mut self, dict_size: usize, buffer: ReadCursor) -> Result<()> {
        let need_size = dict_size + 1;
        // TODO: Maybe try resizing if not shared?
        self.dictionary = Array::new(&self.manager, self.dictionary.datatype().clone(), need_size)?;

        // Read in the plain values.
        let mut decoder = PlainDecoder {
            buffer,
            value_reader: V::default(),
        };
        decoder.read_plain(
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

#[derive(Debug)]
pub struct DictionaryDecoder<V: ValueReader> {
    /// Decoder for dictionary indices.
    rle_decoder: RleBpDecoder,
    /// Reusable buffer to read indice into, used to 'select' the dictionary
    /// array.
    sel_buf: Vec<u32>,
    _v: PhantomData<V>,
}

impl<V> DictionaryDecoder<V>
where
    V: ValueReader,
{
    pub fn new(rle_decoder: RleBpDecoder) -> Self {
        DictionaryDecoder {
            rle_decoder,
            sel_buf: Vec::new(),
            _v: PhantomData,
        }
    }

    pub fn read(
        &mut self,
        dict: &Dictionary<V>,
        definitions: Definitions,
        output: &mut Array,
        offset: usize,
        count: usize,
    ) -> Result<()> {
        // TODO: Possibly do selection instead of copy. Benchmarks needed.

        match definitions {
            Definitions::HasDefinitions { levels, max } => {
                // Read only the indices for valid values.
                let valid_count = levels.iter().filter(|&&def| def == max).count();
                self.sel_buf.resize(valid_count, 0);
                self.rle_decoder.read(&mut self.sel_buf)?;

                // Null is the last value in the dictionary array.
                let null_idx = dict.dictionary.logical_len() - 1;

                // TODO: Slow?
                let mapping = (0..count).map(|idx| {
                    if levels[idx] == max {
                        // Not null
                        let src = self.sel_buf[idx] as usize;
                        (src, idx + offset)
                    } else {
                        // Is null
                        (null_idx, idx + offset)
                    }
                });
                copy_rows_array(&dict.dictionary, mapping, output)?;

                Ok(())
            }
            Definitions::NoDefinitions => {
                self.sel_buf.resize(count, 0);
                self.rle_decoder.read(&mut self.sel_buf)?;

                // Copy from dictionary to dest.
                let mapping = self
                    .sel_buf
                    .iter()
                    .enumerate()
                    .map(|(dest, &src)| (src as usize, dest + offset));
                copy_rows_array(&dict.dictionary, mapping, output)?;

                Ok(())
            }
        }
    }
}
