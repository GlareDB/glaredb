use std::collections::HashMap;

use bytes::Bytes;

use super::decoder::ColumnValueDecoder;
use super::general_err;
use crate::basic::Encoding;
use crate::data_type::DataType;
use crate::encodings::decoding::{get_decoder, Decoder, DictDecoder, PlainDecoder};
use crate::errors::{nyi_err, ParquetResult};
use crate::schema::types::ColumnDescPtr;

/// Column value decoder that uses a Vec for the buffer to write to.
///
/// This is suitable for fixed len primitives, but may have suboptimal
/// performance for variable length data.
#[derive(Debug)]
pub struct BasicColumnValueDecoder<T: DataType> {
    description: ColumnDescPtr,
    /// Current encoding we're reading with.
    ///
    /// Set when we set the page data we're reading.
    current_encoding: Option<Encoding>,
    /// Cache of decoders for existing encodings
    decoders: HashMap<Encoding, Box<dyn Decoder<T>>>,
}

impl<T: DataType> BasicColumnValueDecoder<T> {
    /// Create a new decoder for a column.
    pub fn new(description: &ColumnDescPtr) -> Self {
        Self {
            description: description.clone(),
            current_encoding: None,
            decoders: Default::default(),
        }
    }
}

impl<T: DataType> ColumnValueDecoder for BasicColumnValueDecoder<T> {
    type Buffer = Vec<T::T>;

    fn set_dict(
        &mut self,
        buf: Bytes,
        num_values: u32,
        mut encoding: Encoding,
        _is_sorted: bool,
    ) -> ParquetResult<()> {
        if encoding == Encoding::PLAIN || encoding == Encoding::PLAIN_DICTIONARY {
            encoding = Encoding::RLE_DICTIONARY
        }

        if self.decoders.contains_key(&encoding) {
            return Err(general_err!("Column cannot have more than one dictionary"));
        }

        if encoding == Encoding::RLE_DICTIONARY {
            let mut dictionary = PlainDecoder::<T>::new(self.description.type_length());
            dictionary.set_data(buf, num_values as usize)?;

            let mut decoder = DictDecoder::new();
            decoder.set_dict(Box::new(dictionary))?;
            self.decoders.insert(encoding, Box::new(decoder));
            Ok(())
        } else {
            Err(nyi_err!(
                "Invalid/Unsupported encoding type for dictionary: {}",
                encoding
            ))
        }
    }

    fn set_data(
        &mut self,
        mut encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> ParquetResult<()> {
        use std::collections::hash_map::Entry;

        if encoding == Encoding::PLAIN_DICTIONARY {
            encoding = Encoding::RLE_DICTIONARY;
        }

        let decoder = if encoding == Encoding::RLE_DICTIONARY {
            self.decoders
                .get_mut(&encoding)
                .expect("Decoder for dict should have been set")
        } else {
            // Search cache for data page decoder
            match self.decoders.entry(encoding) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(v) => {
                    let data_decoder = get_decoder::<T>(self.description.clone(), encoding)?;
                    v.insert(data_decoder)
                }
            }
        };

        decoder.set_data(data, num_values.unwrap_or(num_levels))?;
        self.current_encoding = Some(encoding);
        Ok(())
    }

    fn read(&mut self, out: &mut Vec<T::T>, num_values: usize) -> ParquetResult<usize> {
        let encoding = self
            .current_encoding
            .expect("current_encoding should be set");

        let current_decoder = self
            .decoders
            .get_mut(&encoding)
            .unwrap_or_else(|| panic!("decoder for encoding {encoding} should be set"));

        // TODO: Push vec into decoder (#5177)
        let start = out.len();
        out.resize(start + num_values, T::T::default());
        let read = current_decoder.read(&mut out[start..])?;
        out.truncate(start + read);
        Ok(read)
    }

    fn skip_values(&mut self, num_values: usize) -> ParquetResult<usize> {
        let encoding = self
            .current_encoding
            .expect("current_encoding should be set");

        let current_decoder = self
            .decoders
            .get_mut(&encoding)
            .unwrap_or_else(|| panic!("decoder for encoding {encoding} should be set"));

        current_decoder.skip(num_values)
    }
}
