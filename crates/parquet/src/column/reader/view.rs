use bytes::Bytes;

use super::decoder::ColumnValueDecoder;
use super::{ConvertedType, Encoding};
use crate::decoding::view::{PlainViewDecoder, ViewBuffer, ViewDecoder};
use crate::errors::Result;
use crate::schema::types::ColumnDescPtr;

/// Column value decoder for byte arrays that stores bytes in a contiguous
/// buffer with "views" slicing into the buffer for the actual values.
///
/// The "views" correspond to Arrow's string view concept (and to our "german"
/// buffers).
#[derive(Debug)]
pub struct ViewColumnValueDecoder {
    /// Optional deictionary.
    dict: Option<ViewBuffer>,
    /// Current decoder.
    decoder: Option<ViewDecoder>,
    /// If we should validate utf8.
    validate_utf8: bool,
}

impl ViewColumnValueDecoder {
    pub fn new(description: &ColumnDescPtr) -> Self {
        ViewColumnValueDecoder {
            dict: None,
            decoder: None,
            validate_utf8: description.converted_type() == ConvertedType::UTF8,
        }
    }
}

impl ColumnValueDecoder for ViewColumnValueDecoder {
    /// Stores bytes in a contiguous array.
    ///
    /// Note that this will use [u8] for strings as well, string validation is
    /// handled separately, but the underlying storage is the same.
    type Buffer = ViewBuffer;

    fn set_dict(
        &mut self,
        buf: Bytes,
        num_values: u32,
        encoding: super::Encoding,
        _is_sorted: bool,
    ) -> Result<()> {
        if !matches!(
            encoding,
            Encoding::PLAIN | Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY
        ) {
            return Err(nyi_err!(
                "Invalid/Unsupported encoding type for dictionary: {}",
                encoding
            ));
        }

        let mut dict = ViewBuffer::new(num_values as usize);
        PlainViewDecoder::new(
            buf,
            num_values as usize,
            Some(num_values as usize),
            self.validate_utf8,
        )
        .read(&mut dict, num_values as usize)?;

        self.dict = Some(dict);

        Ok(())
    }

    fn set_data(
        &mut self,
        encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()> {
        self.decoder = Some(ViewDecoder::new(
            encoding,
            data,
            num_levels,
            num_values,
            self.validate_utf8,
        )?);
        Ok(())
    }

    fn read(&mut self, out: &mut Self::Buffer, num_values: usize) -> Result<usize> {
        let decoder = self
            .decoder
            .as_mut()
            .ok_or_else(|| general_err!("no decoder set"))?;

        decoder.read(out, num_values, self.dict.as_ref())
    }

    fn skip_values(&mut self, num_values: usize) -> Result<usize> {
        let decoder = self
            .decoder
            .as_mut()
            .ok_or_else(|| general_err!("no decoder set"))?;

        decoder.skip(num_values, self.dict.as_ref())
    }
}
