//! Delta binary packed encoding.
//!
//! See <https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5>

use glaredb_error::Result;

use crate::column::bitutil::{BitUnpackState, bit_unpack, read_unsigned_vlq, zigzag_decode};
use crate::column::read_buffer::ReadCursor;

#[derive(Debug)]
struct DeltaBpDecoder {
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
    min_delta: i64,
    prev_value: i64,
    bit_unpack_state: BitUnpackState,
}

impl DeltaBpDecoder {
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
        let first_val = zigzag_decode(read_unsigned_vlq(&mut cursor)?);

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
            min_delta: 0,
            prev_value: first_val,
            bit_unpack_state: BitUnpackState::new(0),
        })
    }

    fn read(&mut self, out: &mut [i64]) -> Result<()> {
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
            let mut delta_buf = vec![0u64; count];
            bit_unpack(
                &mut self.bit_unpack_state,
                &mut self.cursor,
                &mut delta_buf[..],
            )?;

            for delta in delta_buf {
                let full_delta = self.min_delta + delta as i64;
                self.prev_value += full_delta;
                out[out_idx] = self.prev_value;
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

        self.min_delta = zigzag_decode(read_unsigned_vlq(&mut self.cursor)?);

        // Read mini-block bit widths
        for i in 0..self.mini_block_count {
            self.mini_block_bit_widths[i] = unsafe { self.cursor.read_next_unchecked::<u8>() };
        }

        self.mini_block_idx = 0;
        self.mini_block_value_idx = 0;
        self.bit_unpack_state = BitUnpackState::new(self.mini_block_bit_widths[0]);

        Ok(())
    }
}
