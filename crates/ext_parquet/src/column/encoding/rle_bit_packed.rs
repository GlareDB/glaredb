//! RLE/Bit packing hybrid encoding.
//!
//! See <https://parquet.apache.org/docs/file-format/data-pages/encodings/#run-length-encoding--bit-packing-hybrid-rle--3>

use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalBool,
};
use glaredb_error::{DbError, Result};

use super::Definitions;
use crate::column::bitutil::{BitPackEncodeable, BitUnpackState, bit_unpack, read_unsigned_vlq};
use crate::column::encoding::plain::PlainDecoder;
use crate::column::read_buffer::ReadCursor;
use crate::column::value_reader::ValueReader;

/// Decoder for reading bool values using RLE.
#[derive(Debug)]
pub struct RleBoolDecoder {
    /// Reusable read buffer.
    // TODO: Buffer manager
    buf: Vec<u8>,
    /// Underlying decoder.
    decoder: RleBitPackedDecoder,
}

impl RleBoolDecoder {
    pub fn new(buffer: ReadCursor) -> Self {
        RleBoolDecoder {
            buf: Vec::new(),
            decoder: RleBitPackedDecoder::new(buffer, 1),
        }
    }

    pub fn read(
        &mut self,
        definitions: Definitions,
        output: &mut Array,
        offset: usize,
        count: usize,
    ) -> Result<()> {
        // Only read valid values.
        let read_count = match definitions {
            Definitions::HasDefinitions { levels, max } => {
                let valid_count = levels.iter().filter(|&&def| def == max).count();
                valid_count
            }
            Definitions::NoDefinitions => count,
        };

        self.buf.clear();
        self.buf.resize(read_count, 0);

        self.decoder.read(&mut self.buf)?;

        // Use plain decoder to convert from u8 to bool. Handles
        // definitions/nulls for us.
        //
        // No state is tracked in the casting reader.
        let plain_cursor = ReadCursor::from_slice(&self.buf);
        let mut plain = PlainDecoder {
            buffer: plain_cursor,
            value_reader: RealSizedBoolReader,
        };

        plain.read_plain(definitions, output, offset, count)
    }
}

/// Value reader that reads bools from the cursor.
///
/// This is used since we're unpacking bools into [u8].
#[derive(Debug, Clone, Copy, Default)]
struct RealSizedBoolReader;

const _BOOL_SIZE_ASSERTION: () = assert!(std::mem::size_of::<bool>() == std::mem::size_of::<u8>());

impl ValueReader for RealSizedBoolReader {
    type Storage = PhysicalBool;

    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
    ) {
        let v = unsafe { data.read_next_unchecked::<bool>() };
        out.put(out_idx, &v);
    }

    unsafe fn skip_unchecked(&mut self, data: &mut ReadCursor) {
        unsafe { data.skip_bytes_unchecked(std::mem::size_of::<bool>()) };
    }
}

/// An RLE/bit packing hybrid decoder.
#[derive(Debug)]
pub struct RleBitPackedDecoder {
    /// The underlying buffer for reading.
    buffer: ReadCursor,
    /// Number of bits needed per encoded value.
    bit_width: u8,
    /// The current RLE value (if in an RLE run).
    curr_val: u64,
    /// How many values remain in the current RLE run.
    rle_left: usize,
    /// How many literal (bit-packed) values remain in the current run.
    bit_packed_left: usize,
    /// Current bit position (within the next byte) for literal runs.
    bit_pos: u8,
    /// Number of bytes used to encode a single value.
    byte_enc_len: usize,
}

impl RleBitPackedDecoder {
    pub fn new(buffer: ReadCursor, bit_width: u8) -> Self {
        assert!(bit_width <= 64);
        let byte_enc_len = bit_width.div_ceil(8) as usize;
        RleBitPackedDecoder {
            buffer,
            bit_width,
            curr_val: 0,
            bit_pos: 0,
            rle_left: 0,
            bit_packed_left: 0,
            byte_enc_len,
        }
    }

    /// Reads decoded values into the output slice.
    ///
    /// This method will fill the output slice with decoded values by using either
    /// an RLE (repeated) run or a literal (bit‑packed) run. When no run is active, it
    /// reads the next run indicator.
    pub fn read<T>(&mut self, values: &mut [T]) -> Result<()>
    where
        T: BitPackEncodeable,
    {
        let mut num_read = 0;
        while num_read < values.len() {
            if self.rle_left > 0 {
                // RLE run: fill the output with the current value.
                let count = usize::min(values.len() - num_read, self.rle_left);
                let val = T::from_u64(self.curr_val);
                values[num_read..(num_read + count)].fill(val);
                self.rle_left -= count;
                num_read += count;
            } else if self.bit_packed_left > 0 {
                // Literal run: unpack bits.
                let count = usize::min(values.len() - num_read, self.bit_packed_left);
                let mut state = BitUnpackState::new(self.bit_width);
                // Set the temporary state to the current bit position.
                state.bit_pos = self.bit_pos;
                bit_unpack(
                    &mut state,
                    &mut self.buffer,
                    &mut values[num_read..(num_read + count)],
                )?;
                // Update our state from the temporary state.
                self.bit_pos = state.bit_pos;
                self.bit_packed_left -= count;
                num_read += count;
            } else {
                // No active run: fetch the next run indicator.
                self.read_next()?;
            }
        }
        // Ensure we filled the requested output.
        debug_assert_eq!(num_read, values.len());
        Ok(())
    }

    /// Reads the next run indicator and initializes either an RLE run
    /// or a literal (bit‑packed) run.
    ///
    /// This method assumes that the current bit position is zero (byte-aligned).
    fn read_next(&mut self) -> Result<()> {
        if self.bit_pos != 0 {
            return Err(DbError::new(
                "RleBpDecoder.read_next requires byte alignment",
            ));
        }
        // Read the vlq indicator from the buffer.
        let indicator_val = read_unsigned_vlq(&mut self.buffer)?;
        // If the least-significant bit is 1, this is a literal run.
        if indicator_val & 1 == 1 {
            let num_groups = (indicator_val >> 1) as usize;
            self.bit_packed_left = num_groups * 8;
            // For literal runs the bit_pos remains 0 (byte aligned).
        } else {
            // Otherwise, this is an RLE run.
            self.rle_left = (indicator_val >> 1) as usize;
            // Read the next fixed-width value (spanning byte_enc_len bytes).
            self.curr_val = 0;
            for idx in 0..self.byte_enc_len {
                let b = unsafe { self.buffer.read_next_unchecked::<u8>() } as u64;
                self.curr_val |= b << (idx * 8);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use glaredb_core::buffer::buffer_manager::NopBufferManager;

    use super::*;
    use crate::column::read_buffer::OwnedReadBuffer;

    #[test]
    fn rlebp_literal() {
        // bit_width = 1, one literal group of 8 sawtooth bits:
        // VLQ=0x03
        let raw = [(1 << 1) | 1, 0b10101010];
        let mut buf = OwnedReadBuffer::from_bytes(&NopBufferManager, raw).unwrap();
        let cursor = buf.take_remaining();
        let mut decoder = RleBitPackedDecoder::new(cursor, 1);
        let mut out = [0u8; 8];
        decoder.read(&mut out).unwrap();
        assert_eq!(out, [0, 1, 0, 1, 0, 1, 0, 1]);
    }

    #[test]
    fn rlebp_decoder_rle() {
        // Test RLE-only run.

        // Bit width = 1
        // Run indicator (vlq) has the LSB = 0
        // Repeated_count = 8,
        // Indicator value = (8 << 1) = 16 (0x10).
        // Repeated value in one byte, 4 (0x04).
        // Final stream: [0x10, 0x01]
        let raw = [0x10, 0x04];
        let mut buf = OwnedReadBuffer::from_bytes(&NopBufferManager, raw).unwrap();
        let cursor = buf.take_remaining();
        let mut decoder = RleBitPackedDecoder::new(cursor, 1);
        let mut output = [0u8; 8];
        decoder.read(&mut output).unwrap();
        assert_eq!(output, [4, 4, 4, 4, 4, 4, 4, 4]);
    }

    #[test]
    fn rlebp_decoder_literal() {
        // Test literal-only run.

        // bit width = 3.
        // Run indicator (vlq) has the LSB = 1, indicator = 0x03
        // Literal group of 8 values: [1,2,3,4,5,6,7,0]
        // Little endian:
        //   1 + (2<<3) + (3<<6) + (4<<9) + (5<<12) + (6<<15) + (7<<18) + (0<<21)
        // => 0x1F58D1 => [0xD1, 0x58, 0x1F]
        // Final stream: [0x03, 0xD1, 0x58, 0x1F].
        let raw = [0x03, 0xD1, 0x58, 0x1F];
        let mut buf = OwnedReadBuffer::from_bytes(&NopBufferManager, raw).unwrap();
        let cursor = buf.take_remaining();
        let mut decoder = RleBitPackedDecoder::new(cursor, 3);
        let mut output = [0u8; 8];
        decoder.read(&mut output).expect("read failed");
        assert_eq!(output, [1, 2, 3, 4, 5, 6, 7, 0]);
    }

    #[test]
    fn rlebp_decoder_combined() {
        // Test a combined run: first an RLE run then a literal run.

        // For bit_width=2:
        //
        // - RLE run (5 vals)
        //   VLQ indicator for RLE (LSB=0) = (5 << 1) = 10 (0x0A).
        //   Repeated val = 2 (0x02).
        //
        // - Literal run (1 group)
        //   VLQ indicator (LSB=1) for 1 group = (1 << 1) | 1 = 3 (0x03)
        //   2-bit literal values: [0, 1, 2, 3, 0, 1, 2, 3]
        //   Since each value is 2 bits, these 8 values occupy 16 bits (2 bytes).
        //   The 16‑bit concatenation is:
        //     v0 + (v1<<2) + (v2<<4) + (v3<<6) + (v4<<8) + (v5<<10) + (v6<<12) + (v7<<14),
        //   where v0=0, v1=1, v2=2, v3=3, v4=0, v5=1, v6=2, v7=3.
        //   LE bytes =  [0xE4, 0xE4].
        //
        // Final stream: [0x0A, 0x02, 0x03, 0xE4, 0xE4].
        let raw = [0x0A, 0x02, 0x03, 0xE4, 0xE4];
        let mut buf = OwnedReadBuffer::from_bytes(&NopBufferManager, raw).unwrap();
        let cursor = buf.take_remaining();
        let mut decoder = RleBitPackedDecoder::new(cursor, 2);
        // Expect 5 RLE values of 2 followed by 8 literal values.
        let mut output = [0u8; 13];
        decoder.read(&mut output).unwrap();
        let expected: [u8; 13] = [2, 2, 2, 2, 2, 0, 1, 2, 3, 0, 1, 2, 3];
        assert_eq!(output, expected);
    }
}
