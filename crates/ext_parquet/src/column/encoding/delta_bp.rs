//! Delta binary packed encoding.
//!
//! See <https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5>

use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_core::arrays::array::Array;
use glaredb_error::{DbError, Result};
use num::{FromPrimitive, Zero};

use super::Definitions;
use crate::column::bitutil::{
    BitPackEncodeable,
    BitUnpackState,
    bit_unpack,
    read_unsigned_vlq,
    zigzag_decode,
};
use crate::column::encoding::plain::PlainDecoder;
use crate::column::read_buffer::ReadCursor;
use crate::column::value_reader::ValueReader;

// TODO: We can probably attach the physical type to the value reader instead of
// need to have it as separate generic.
#[derive(Debug)]
pub struct DeltaBpDecoder<T, V>
where
    V: ValueReader,
{
    // TODO: Make this buffer managed, probably with a typed buffer.
    delta_buffer: Vec<T>,
    inner: DeltaBpDecoderInner<T>,
    _v: PhantomData<V>,
}

impl<T, V> DeltaBpDecoder<T, V>
where
    T: FromPrimitive + Zero + std::ops::Add + Copy + BitPackEncodeable + Debug,
    V: ValueReader,
{
    pub fn try_new(cursor: ReadCursor) -> Result<Self> {
        let inner = DeltaBpDecoderInner::try_new(cursor)?;

        Ok(DeltaBpDecoder {
            delta_buffer: Vec::new(),
            inner,
            _v: PhantomData,
        })
    }

    pub fn read(
        &mut self,
        definitions: Definitions,
        output: &mut Array,
        offset: usize,
        count: usize,
    ) -> Result<()> {
        let read_count = match definitions {
            Definitions::HasDefinitions { levels, max } => {
                let valid_count = levels.iter().filter(|&&def| def == max).count();
                valid_count
            }
            Definitions::NoDefinitions => count,
        };

        self.delta_buffer.clear();
        self.delta_buffer.resize(read_count, T::zero());

        self.inner.read(&mut self.delta_buffer)?;

        // We've read into our local buffer, use a plain decoder to read it into
        // the output array.
        //
        // This handles nulls for us, as well as any casting that needs to
        // happen, as the delta encoder should only be called i32 or i64, but
        // the logical type might be a different width.
        //
        // Nor do we need to worry about value reader state (since reading
        // i32/i64 doesn't require anything).
        let read_cursor = ReadCursor::from_slice(&self.delta_buffer);
        let mut plain = PlainDecoder {
            buffer: read_cursor,
            value_reader: V::default(),
        };

        // Use original offset and count.
        plain.read_plain(definitions, output, offset, count)
    }
}

#[derive(Debug)]
struct DeltaBpDecoderInner<T> {
    /// Cursor we're reading from.
    cursor: ReadCursor,
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
    /// Number of values per miniblock.
    values_per_mini_block: usize,
    /// Minimum delta for the current miniblock we're on.
    min_delta: T,
    /// Previous value we computed.
    prev_value: T,
    bit_unpack_state: BitUnpackState,
}

impl<T> DeltaBpDecoderInner<T>
where
    T: FromPrimitive + Zero + std::ops::Add + Copy + BitPackEncodeable + Debug,
{
    fn try_new(mut cursor: ReadCursor) -> Result<Self> {
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

        let mut inner = Self {
            cursor,
            mini_block_count,
            values_remaining: total_values - 1, // We've already "decoded" the first value
            mini_block_bit_widths: vec![0; mini_block_count],
            mini_block_idx: 0,
            mini_block_value_idx: 0,
            values_per_mini_block,
            min_delta: T::zero(),
            prev_value: first_val,
            bit_unpack_state: BitUnpackState::new(0),
        };

        // Load the first block.
        inner.load_next_block()?;

        Ok(inner)
    }

    fn read(&mut self, out: &mut [T]) -> Result<()> {
        if out.is_empty() {
            return Ok(());
        }

        let mut out_idx = 0;
        // Set the "first" value for this output.
        out[0] = self.prev_value;
        out_idx += 1;

        while out_idx < out.len() && self.values_remaining > 0 {
            if self.mini_block_value_idx >= self.values_per_mini_block
                || self.mini_block_idx >= self.mini_block_count
            {
                self.load_next_block()?;
            }

            // Start of a new mini‑block?
            if self.mini_block_value_idx == 0 {
                let bit_width = self.mini_block_bit_widths[self.mini_block_idx];
                self.bit_unpack_state = BitUnpackState::new(bit_width);
            }

            let rem_cap = out.len() - out_idx;
            let rem_in_mini_block = self.values_per_mini_block - self.mini_block_value_idx;
            let count = usize::min(rem_cap, rem_in_mini_block);
            let unpack_buf = &mut out[out_idx..(out_idx + count)];

            bit_unpack(&mut self.bit_unpack_state, &mut self.cursor, unpack_buf)?;

            for output in unpack_buf {
                // Final value is unpacked delta + min delta + prev value.
                let final_v = *output + self.min_delta + self.prev_value;
                *output = final_v;
                self.prev_value = final_v;
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
