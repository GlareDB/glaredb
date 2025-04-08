//! RLE/Bit packing hybrid encoding.
//!
//! See <https://parquet.apache.org/docs/file-format/data-pages/encodings/#run-length-encoding--bit-packing-hybrid-rle--3>

use glaredb_error::Result;

use crate::column::bitutil::{BitPackEncodeable, BitUnpacker};
use crate::column::read_buffer::ReadBuffer;

/// An RLE/bit packing hybrid decoder.
#[derive(Debug)]
pub struct RleBpDecoder {
    buffer: ReadBuffer,
    /// Bits needed to encode the value.
    bit_width: u8,
    /// Current value.
    curr_val: u64,
    /// Remaining number of RLE values in this run.
    rle_left: usize,
    /// Remaining number of bit packaged values in this run.
    bit_packed_left: usize,
    /// Current bit position we're scanning.
    bit_pos: u8,
    /// Div ceil for how many bytes it takes to encode a single value.
    byte_enc_len: usize,
}

impl RleBpDecoder {
    pub fn new(buffer: ReadBuffer, bit_width: u8) -> Self {
        assert!(bit_width <= 64);

        let byte_enc_len = bit_width.div_ceil(8) as usize;

        RleBpDecoder {
            buffer,
            bit_width,
            curr_val: 0,
            bit_pos: 0,
            rle_left: 0,
            bit_packed_left: 0,
            byte_enc_len,
        }
    }

    pub fn get_batch<T>(&mut self, values: &mut [T]) -> Result<()>
    where
        T: BitPackEncodeable,
    {
        let mut num_read = 0;

        while num_read < values.len() {
            if self.rle_left > 0 {
                let num_vals = usize::min(values.len() - num_read, self.rle_left);
                let val = T::from_u64(self.curr_val);

                values[num_read..(num_read + num_vals)].fill(val);

                self.rle_left -= num_vals;
                num_read += num_vals;
            } else if self.bit_packed_left > 0 {
                let num_vals = usize::min(values.len() - num_read, self.bit_packed_left);

                let mut unpacker = BitUnpacker {
                    buf: &mut self.buffer,
                    bit_pos: self.bit_pos,
                    bit_width: self.bit_width,
                };
                unpacker.unpack(&mut values[num_read..(num_read + num_vals)]);

                self.bit_pos = unpacker.bit_pos;
                self.bit_packed_left -= num_vals;
                num_read += num_vals;
            } else {
                self.read_next()?;
            }
        }

        // We should be passing in exact buffer sizes.
        debug_assert_eq!(values.len(), num_read);

        Ok(())
    }

    fn read_next(&mut self) -> Result<()> {
        let mut unpacker = BitUnpacker {
            buf: &mut self.buffer,
            bit_pos: self.bit_pos,
            bit_width: self.bit_width,
        };

        // lsb indicates if it is a literal run or repeated run.
        let indicator_val = unpacker.read_vlq_i64()?;
        self.bit_pos = unpacker.bit_pos;

        if indicator_val & 1 == 1 {
            self.bit_packed_left = ((indicator_val >> 1) * 8) as usize;
        } else {
            self.rle_left = (indicator_val >> 1) as usize;

            // Read the next set of bytes to get the new current value.
            self.curr_val = 0;
            for idx in 0..self.byte_enc_len {
                let b = unsafe { self.buffer.read_next_unchecked::<u8>() } as u64;
                self.curr_val |= b << (idx * 8);
            }
        }

        Ok(())
    }
}
