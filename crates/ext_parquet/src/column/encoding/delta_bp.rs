//! Delta binary packed encoding.
//!
//! See <https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5>

use glaredb_error::{DbError, Result};
use num::{FromPrimitive, Zero};

use crate::column::bitutil::{
    BitPackEncodeable,
    BitUnpackState,
    bit_unpack,
    read_unsigned_vlq,
    zigzag_decode,
};
use crate::column::read_buffer::ReadCursor;

#[derive(Debug)]
struct DeltaBpDecoderInner<T> {
    /// Cursor we're reading from.
    cursor: ReadCursor,
    /// Block size.
    block_size: usize,
    /// Number of miniblocks.
    mini_block_count: usize,
    /// Number of values remaining overall.
    values_remaining: usize,
    /// Bit widths per miniblock.
    mini_block_bit_widths: Vec<u8>,
    /// Current miniblock we're on.
    mini_block_idx: usize,
    /// Current value within the miniblock we're on.
    mini_block_value_idx: usize,
    /// Idk, I put this here just in case.
    values_in_current_block: usize,
    /// Number of values per miniblock.
    values_per_mini_block: usize,
    min_delta: T,
    prev_value: T,
    bit_unpack_state: BitUnpackState,
}

impl<T> DeltaBpDecoderInner<T>
where
    T: FromPrimitive + Zero + std::ops::Add + Copy + BitPackEncodeable,
{
    fn new(mut cursor: ReadCursor) -> Result<Self> {
        // Header
        // <block size in values>
        // <number of miniblocks in a block>
        // <total value count>
        // <first value>
        //
        // - the block size is a multiple of 128; it is stored as a ULEB128 int
        // - the miniblock count per block is a divisor of the block size such
        //   that their quotient, the number of values in a miniblock, is a
        //   multiple of 32; it is stored as a ULEB128 int
        // - the total value count is stored as a ULEB128 int
        // - the first value is stored as a zigzag ULEB128 int

        let block_size = read_unsigned_vlq(&mut cursor)? as usize; // uleb128
        let mini_block_count = read_unsigned_vlq(&mut cursor)? as usize;
        let total_values = read_unsigned_vlq(&mut cursor)? as usize;
        let first_val = T::from_i64(zigzag_decode(read_unsigned_vlq(&mut cursor)?))
            .ok_or_else(|| DbError::new("first value too large"))?;

        let values_per_mini_block = block_size / mini_block_count;

        Ok(Self {
            cursor,
            block_size,
            mini_block_count,
            values_remaining: total_values,
            mini_block_bit_widths: vec![0; mini_block_count],
            mini_block_idx: 0,
            mini_block_value_idx: 0,
            values_in_current_block: 0,
            values_per_mini_block,
            min_delta: T::zero(),
            prev_value: first_val,
            bit_unpack_state: BitUnpackState::new(0),
        })
    }

    fn read(&mut self, out: &mut [T]) -> Result<()> {
        if out.is_empty() {
            return Ok(());
        }

        let mut out_idx = 0;

        while out_idx < out.len() && self.values_remaining > 0 {
            if self.mini_block_value_idx >= self.values_per_mini_block
                || self.mini_block_idx >= self.mini_block_count
            {
                self.load_next_block()?;
            }

            let bit_width = self.mini_block_bit_widths[self.mini_block_idx];
            self.bit_unpack_state.bit_width = bit_width;

            let count = usize::min(
                out.len() - out_idx,
                self.values_per_mini_block - self.mini_block_value_idx,
            );
            let unpack_buf = &mut out[out_idx..count];

            bit_unpack(&mut self.bit_unpack_state, &mut self.cursor, unpack_buf)?;

            for delta in unpack_buf {
                // Final value is unpacked delta + min delta + prev value.
                *delta = *delta + self.min_delta + self.prev_value;
                self.prev_value = *delta;
                out_idx += 1;
                self.values_remaining -= 1;
                self.mini_block_value_idx += 1;
            }

            if self.mini_block_value_idx >= self.values_per_mini_block {
                self.mini_block_idx += 1;
                self.mini_block_value_idx = 0;
            }
        }

        Ok(())
    }

    fn load_next_block(&mut self) -> Result<()> {
        // Block header
        // <min delta>
        // <list of bitwidths of miniblocks>
        // <miniblocks>
        //
        // - the min delta is a zigzag ULEB128 int (we compute a minimum as we
        //   need positive integers for bit packing)
        // - the bitwidth of each block is stored as a byte
        // - each miniblock is a list of bit packed ints according to the bit
        //   width stored at the begining of the block

        self.min_delta = T::from_i64(zigzag_decode(read_unsigned_vlq(&mut self.cursor)?))
            .ok_or_else(|| DbError::new("Min delta too large"))?;

        // read mini-block bit widths
        for i in 0..self.mini_block_count {
            self.mini_block_bit_widths[i] = unsafe { self.cursor.read_next_unchecked::<u8>() };
        }

        self.mini_block_idx = 0;
        self.mini_block_value_idx = 0;
        self.bit_unpack_state = BitUnpackState::new(self.mini_block_bit_widths[0]);

        Ok(())
    }
}
