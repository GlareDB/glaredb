//! Delta binary packed encoding.
//!
//! See <https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5>

use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_core::arrays::array::Array;
use glaredb_error::{DbError, Result};
use num::traits::WrappingAdd;
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
pub struct DeltaBinaryPackedDecoder<T, V>
where
    V: ValueReader,
{
    // TODO: Make this buffer managed, probably with a typed buffer.
    delta_buffer: Vec<T>,
    inner: DeltaBinaryPackedValueDecoder<T>,
    _v: PhantomData<V>,
}

impl<T, V> DeltaBinaryPackedDecoder<T, V>
where
    T: FromPrimitive + Zero + WrappingAdd + Copy + BitPackEncodeable + Debug,
    V: ValueReader,
{
    pub fn try_new(cursor: ReadCursor) -> Result<Self> {
        let inner = DeltaBinaryPackedValueDecoder::try_new(cursor)?;

        Ok(DeltaBinaryPackedDecoder {
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
                debug_assert_eq!(levels.len(), count);

                levels.iter().filter(|&&def| def == max).count()
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
pub(crate) struct DeltaBinaryPackedValueDecoder<T> {
    /// Cursor we're reading from.
    cursor: ReadCursor,
    /// Number of miniblocks.
    mini_block_count: usize,
    /// Total number of values in the buffer.
    total_values: usize,
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

impl<T> DeltaBinaryPackedValueDecoder<T>
where
    T: FromPrimitive + Zero + WrappingAdd + Copy + BitPackEncodeable + Debug,
{
    /// Try to create a new decoder using the provide cursor.
    ///
    /// This will attempt to read the header values and the first miniblock.
    pub fn try_new(mut cursor: ReadCursor) -> Result<Self> {
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

        // We've already "decoded" the first value.
        //
        // Note that we use saturating sub since it's valid to have no values in
        // this buffer.
        let values_remaining = total_values.saturating_sub(1);

        let mut inner = Self {
            cursor,
            mini_block_count,
            total_values,
            values_remaining,
            mini_block_bit_widths: vec![0; mini_block_count],
            mini_block_idx: 0,
            mini_block_value_idx: 0,
            values_per_mini_block,
            min_delta: T::zero(),
            prev_value: first_val,
            bit_unpack_state: BitUnpackState::new(0),
        };

        // Load the first block only if we have values to load.
        if total_values > 0 {
            inner.load_next_block()?;
        }

        Ok(inner)
    }

    pub fn total_values(&self) -> usize {
        self.total_values
    }

    /// Read `out` values.
    ///
    /// `out` should be a known length.
    // TODO: What happens if `out` isn't a known length? What if it's larger
    // than the actual number of values?
    pub fn read(&mut self, out: &mut [T]) -> Result<()> {
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
                //
                // We want to allow overflow. From the spec:
                //
                // > Subtractions in steps 1) and 2) may incur signed arithmetic
                // > overflow, and so will the corresponding additions when
                // > decoding. Overflow should be allowed and handled as wrapping
                // > around in 2's complement notation so that the original values
                // > are correctly restituted. This may require explicit care in
                // > some programming languages (for example by doing all
                // > arithmetic in the unsigned domain).
                let final_v = output
                    .wrapping_add(&self.min_delta)
                    .wrapping_add(&self.prev_value);
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

    /// Try to get the underlying cursor from this decoder, advancing the cursor
    /// past the end of the last miniblock.
    pub fn try_into_cursor(mut self) -> Result<ReadCursor> {
        // TODO: Error if we're not actually in the last miniblock. This method
        // should only be called when reading the lengths for the other delta...
        // encodings which should always read the full set of value.

        // > If there are not enough values to fill the last miniblock, we pad
        // > the miniblock so that its length is always the number of values in
        // > a full miniblock multiplied by the bit width. The values of the
        // > padding bits should be zero, but readers must accept paddings
        // > consisting of arbitrary bits as well.

        let idx = self.mini_block_idx;
        let pos_in_mb = self.mini_block_value_idx;
        let per_mb = self.values_per_mini_block;
        let width = self.mini_block_bit_widths[idx] as usize;

        let rem_vals = per_mb - pos_in_mb;
        if width > 0 && rem_vals > 0 {
            let bits_to_skip = width * rem_vals;
            let bytes_to_skip = bits_to_skip.div_ceil(8);
            unsafe {
                self.cursor.skip_bytes_unchecked(bytes_to_skip);
            }
        }

        Ok(self.cursor)
    }

    /// Load the next block.
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
