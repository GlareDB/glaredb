// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use bytes::Bytes;

use crate::basic::Encoding;
use crate::encodings::rle::RleDecoder;
use crate::errors::ParquetResult;
use crate::util::bit_util::{num_required_bits, BitReader};

/// Decodes level data
pub trait ColumnLevelDecoder {
    type Buffer;

    /// Set data for this [`ColumnLevelDecoder`]
    fn set_data(&mut self, encoding: Encoding, data: Bytes);
}

/// Buffer for reading decoded values into.
///
/// The only thing we need to know about the buffer is how many values can be
/// written to it. This determines the max number of values we read for a column
/// before requiring a new buffer.
///
/// The decoder implementation will handle the actual decoding of the values.
pub trait ColumnValueBuffer {
    fn len(&self) -> usize;
}

impl<T> ColumnValueBuffer for [T] {
    fn len(&self) -> usize {
        (*self).len()
    }
}

/// Decode column value data.
pub trait ColumnValueDecoder {
    /// Buffer passed into `read` for filling up buffer values.
    type Buffer;

    /// Set the current dictionary page
    fn set_dict(
        &mut self,
        buf: Bytes,
        num_values: u32,
        encoding: Encoding,
        _is_sorted: bool,
    ) -> ParquetResult<()>;

    /// Set the current data page
    ///
    /// - `encoding` - the encoding of the page
    /// - `data` - a point to the page's uncompressed value data
    /// - `num_levels` - the number of levels contained within the page, i.e. values including nulls
    /// - `num_values` - the number of non-null values contained within the page (V2 page only)
    ///
    /// Note: data encoded with [`Encoding::RLE`] may not know its exact length, as the final
    /// run may be zero-padded. As such if `num_values` is not provided (i.e. `None`),
    /// subsequent calls to `ColumnValueDecoder::read` may yield more values than
    /// non-null definition levels within the page
    fn set_data(
        &mut self,
        encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> ParquetResult<()>;

    /// Read values into `out`.
    ///
    /// The length of `out` determines the max number of values to read.
    fn read(&mut self, out: &mut Self::Buffer, num_values: usize) -> ParquetResult<usize>;

    /// Skips over `num_values` values
    ///
    /// Returns the number of values skipped
    fn skip_values(&mut self, num_values: usize) -> ParquetResult<usize>;
}

const SKIP_BUFFER_SIZE: usize = 1024;

#[derive(Debug)]
enum LevelDecoder {
    Packed(BitReader, u8),
    Rle(RleDecoder),
}

impl LevelDecoder {
    fn new(encoding: Encoding, data: Bytes, bit_width: u8) -> Self {
        match encoding {
            Encoding::RLE => {
                let mut decoder = RleDecoder::new(bit_width);
                decoder.set_data(data);
                Self::Rle(decoder)
            }
            #[allow(deprecated)]
            Encoding::BIT_PACKED => Self::Packed(BitReader::new(data), bit_width),
            _ => unreachable!("invalid level encoding: {}", encoding),
        }
    }

    fn read(&mut self, out: &mut [i16]) -> ParquetResult<usize> {
        match self {
            Self::Packed(reader, bit_width) => {
                Ok(reader.get_batch::<i16>(out, *bit_width as usize))
            }
            Self::Rle(reader) => Ok(reader.get_batch(out)?),
        }
    }
}

/// An implementation of [`DefinitionLevelDecoder`] for `[i16]`
#[derive(Debug)]
pub struct DefinitionLevelDecoder {
    decoder: Option<LevelDecoder>,
    bit_width: u8,
    max_level: i16,
}

impl DefinitionLevelDecoder {
    pub fn new(max_level: i16) -> Self {
        let bit_width = num_required_bits(max_level as u64);
        Self {
            decoder: None,
            bit_width,
            max_level,
        }
    }

    /// Read up to `num_levels` definition levels into `out`
    ///
    /// Returns the number of values skipped, and the number of levels skipped
    ///
    /// # Panics
    ///
    /// Implementations may panic if `range` overlaps with already written data
    pub fn read_def_levels(
        &mut self,
        out: &mut Vec<i16>,
        num_levels: usize,
    ) -> ParquetResult<(usize, usize)> {
        // TODO: Push vec into decoder (#5177)
        let start = out.len();
        out.resize(start + num_levels, 0);
        let levels_read = self.decoder.as_mut().unwrap().read(&mut out[start..])?;
        out.truncate(start + levels_read);

        let iter = out.iter().skip(start);
        let values_read = iter.filter(|x| **x == self.max_level).count();
        Ok((values_read, levels_read))
    }

    /// Skips over `num_levels` definition levels
    ///
    /// Returns the number of values skipped, and the number of levels skipped
    pub fn skip_def_levels(&mut self, num_levels: usize) -> ParquetResult<(usize, usize)> {
        let mut level_skip = 0;
        let mut value_skip = 0;
        let mut buf: Vec<i16> = vec![];
        while level_skip < num_levels {
            let remaining_levels = num_levels - level_skip;

            let to_read = remaining_levels.min(SKIP_BUFFER_SIZE);
            buf.resize(to_read, 0);
            let (values_read, levels_read) = self.read_def_levels(&mut buf, to_read)?;
            if levels_read == 0 {
                // Reached end of page
                break;
            }

            level_skip += levels_read;
            value_skip += values_read;
        }

        Ok((value_skip, level_skip))
    }
}

impl ColumnLevelDecoder for DefinitionLevelDecoder {
    type Buffer = Vec<i16>;

    fn set_data(&mut self, encoding: Encoding, data: Bytes) {
        self.decoder = Some(LevelDecoder::new(encoding, data, self.bit_width))
    }
}

pub(crate) const REPETITION_LEVELS_BATCH_SIZE: usize = 1024;

/// An implementation of [`RepetitionLevelDecoder`] for `[i16]`
#[derive(Debug)]
pub struct RepetitionLevelDecoder {
    decoder: Option<LevelDecoder>,
    bit_width: u8,
    buffer: Box<[i16; REPETITION_LEVELS_BATCH_SIZE]>,
    buffer_len: usize,
    buffer_offset: usize,
    has_partial: bool,
}

impl RepetitionLevelDecoder {
    pub fn new(max_level: i16) -> Self {
        let bit_width = num_required_bits(max_level as u64);
        Self {
            decoder: None,
            bit_width,
            buffer: Box::new([0; REPETITION_LEVELS_BATCH_SIZE]),
            buffer_offset: 0,
            buffer_len: 0,
            has_partial: false,
        }
    }

    /// Read up to `max_records` of repetition level data into `out` returning the number
    /// of complete records and levels read
    ///
    /// A record only ends when the data contains a subsequent repetition level of 0,
    /// it is therefore left to the caller to delimit the final record in a column
    ///
    /// # Panics
    ///
    /// Implementations may panic if `range` overlaps with already written data
    pub fn read_rep_levels(
        &mut self,
        out: &mut Vec<i16>,
        num_records: usize,
        num_levels: usize,
    ) -> ParquetResult<(usize, usize)> {
        let mut total_records_read = 0;
        let mut total_levels_read = 0;

        while total_records_read < num_records && total_levels_read < num_levels {
            if self.buffer_len == self.buffer_offset {
                self.fill_buf()?;
                if self.buffer_len == 0 {
                    break;
                }
            }

            let (partial, records_read, levels_read) = self.count_records(
                num_records - total_records_read,
                num_levels - total_levels_read,
            );

            out.extend_from_slice(
                &self.buffer[self.buffer_offset..self.buffer_offset + levels_read],
            );

            total_levels_read += levels_read;
            total_records_read += records_read;
            self.buffer_offset += levels_read;
            self.has_partial = partial;
        }
        Ok((total_records_read, total_levels_read))
    }

    /// Skips over up to `num_levels` repetition levels corresponding to `num_records` records,
    /// where a record is delimited by a repetition level of 0
    ///
    /// Returns the number of records skipped, and the number of levels skipped
    ///
    /// A record only ends when the data contains a subsequent repetition level of 0,
    /// it is therefore left to the caller to delimit the final record in a column
    pub fn skip_rep_levels(
        &mut self,
        num_records: usize,
        num_levels: usize,
    ) -> ParquetResult<(usize, usize)> {
        let mut total_records_read = 0;
        let mut total_levels_read = 0;

        while total_records_read < num_records && total_levels_read < num_levels {
            if self.buffer_len == self.buffer_offset {
                self.fill_buf()?;
                if self.buffer_len == 0 {
                    break;
                }
            }

            let (partial, records_read, levels_read) = self.count_records(
                num_records - total_records_read,
                num_levels - total_levels_read,
            );

            total_levels_read += levels_read;
            total_records_read += records_read;
            self.buffer_offset += levels_read;
            self.has_partial = partial;
        }
        Ok((total_records_read, total_levels_read))
    }

    /// Flush any partially read or skipped record
    pub fn flush_partial(&mut self) -> bool {
        std::mem::take(&mut self.has_partial)
    }

    fn fill_buf(&mut self) -> ParquetResult<()> {
        let read = self.decoder.as_mut().unwrap().read(self.buffer.as_mut())?;
        self.buffer_offset = 0;
        self.buffer_len = read;
        Ok(())
    }

    /// Inspects the buffered repetition levels in the range `self.buffer_offset..self.buffer_len`
    /// and returns the number of "complete" records along with the corresponding number of values
    ///
    /// A "complete" record is one where the buffer contains a subsequent repetition level of 0
    fn count_records(&mut self, records_to_read: usize, num_levels: usize) -> (bool, usize, usize) {
        let mut records_read = 0;

        let levels = num_levels.min(self.buffer_len - self.buffer_offset);
        let buf = self.buffer.iter().skip(self.buffer_offset);
        for (idx, item) in buf.take(levels).enumerate() {
            if *item == 0 && (idx != 0 || self.has_partial) {
                records_read += 1;

                if records_read == records_to_read {
                    return (false, records_read, idx);
                }
            }
        }
        // Either ran out of space in `num_levels` or data in `self.buffer`
        (true, records_read, levels)
    }
}

impl ColumnLevelDecoder for RepetitionLevelDecoder {
    type Buffer = Vec<i16>;

    fn set_data(&mut self, encoding: Encoding, data: Bytes) {
        self.decoder = Some(LevelDecoder::new(encoding, data, self.bit_width));
        self.buffer_len = 0;
        self.buffer_offset = 0;
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::*;

    use super::*;
    use crate::encodings::rle::RleEncoder;

    #[test]
    fn test_skip_padding() {
        let mut encoder = RleEncoder::new(1, 1024);
        encoder.put(0);
        (0..3).for_each(|_| encoder.put(1));
        let data = Bytes::from(encoder.consume());

        let mut decoder = RepetitionLevelDecoder::new(1);
        decoder.set_data(Encoding::RLE, data.clone());
        let (_, levels) = decoder.skip_rep_levels(100, 4).unwrap();
        assert_eq!(levels, 4);

        // The length of the final bit packed run is ambiguous, so without the correct
        // levels limit, it will decode zero padding
        let mut decoder = RepetitionLevelDecoder::new(1);
        decoder.set_data(Encoding::RLE, data);
        let (_, levels) = decoder.skip_rep_levels(100, 6).unwrap();
        assert_eq!(levels, 6);
    }

    #[test]
    fn test_skip_rep_levels() {
        for _ in 0..10 {
            let mut rng = thread_rng();
            let total_len = 10000_usize;
            let mut encoded: Vec<i16> = (0..total_len).map(|_| rng.gen_range(0..5)).collect();
            encoded[0] = 0;
            let mut encoder = RleEncoder::new(3, 1024);
            for v in &encoded {
                encoder.put(*v as _)
            }
            let data = Bytes::from(encoder.consume());

            let mut decoder = RepetitionLevelDecoder::new(5);
            decoder.set_data(Encoding::RLE, data);

            let total_records = encoded.iter().filter(|x| **x == 0).count();
            let mut remaining_records = total_records;
            let mut remaining_levels = encoded.len();
            loop {
                let skip = rng.gen_bool(0.5);
                let records = rng.gen_range(1..=remaining_records.min(5));
                let (records_read, levels_read) = if skip {
                    decoder.skip_rep_levels(records, remaining_levels).unwrap()
                } else {
                    let mut decoded = Vec::new();
                    let (records_read, levels_read) = decoder
                        .read_rep_levels(&mut decoded, records, remaining_levels)
                        .unwrap();

                    assert_eq!(
                        decoded,
                        encoded[encoded.len() - remaining_levels..][..levels_read]
                    );
                    (records_read, levels_read)
                };

                remaining_levels = remaining_levels.checked_sub(levels_read).unwrap();
                if remaining_levels == 0 {
                    assert_eq!(records_read + 1, records);
                    assert_eq!(records, remaining_records);
                    break;
                }
                assert_eq!(records_read, records);
                remaining_records -= records;
                assert_ne!(remaining_records, 0);
            }
        }
    }
}
