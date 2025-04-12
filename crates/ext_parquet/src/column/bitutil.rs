use glaredb_error::{DbError, Result};

use super::read_buffer::ReadCursor;

/// All possible masks for an 8-byte wide value.
/// BITPACK_MASKS[n] = (1 << n) - 1
pub const BITPACK_MASKS: [u64; 65] = {
    let mut masks = [0; 65];
    let mut i = 0;
    while i < 64 {
        masks[i] = (1u64 << i) - 1;
        i += 1;
    }
    masks[64] = u64::MAX;
    masks
};

/// Used for bit extraction; byte = 8 bits
pub const BYTE_WIDTH: u8 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitUnpackState {
    pub bit_pos: u8,
    pub bit_width: u8,
}

impl BitUnpackState {
    pub const fn new(bit_width: u8) -> Self {
        BitUnpackState {
            bit_pos: 0,
            bit_width,
        }
    }
}

/// Unpacks values from the underlying bit‑packed cursor, writing them to `out`.
///
/// This will read `out.len()` values. This will update the bit position in the
/// state to allow resuming partial byte reads.
pub fn bit_unpack<T>(
    state: &mut BitUnpackState,
    cursor: &mut ReadCursor,
    out: &mut [T],
) -> Result<()>
where
    T: BitPackEncodeable,
{
    let mask = BITPACK_MASKS[state.bit_width as usize];

    for dst in out {
        // Read the current byte without advancing.
        let current = unsafe { cursor.peek_next_unchecked::<u8>() };
        let mut v = ((current >> state.bit_pos) as u64) & mask;
        state.bit_pos += state.bit_width;
        // If the new bit position goes past the end of the byte, read subsequent bytes.
        while state.bit_pos > BYTE_WIDTH {
            // Advance one byte.
            let _ = unsafe { cursor.read_next_unchecked::<u8>() };
            let next = unsafe { cursor.peek_next_unchecked::<u8>() };
            let shift_amount = BYTE_WIDTH - (state.bit_pos - state.bit_width);
            v |= ((next as u64) << shift_amount) & mask;
            state.bit_pos -= BYTE_WIDTH;
        }
        // If we've exactly consumed a byte, advance the cursor.
        if state.bit_pos == BYTE_WIDTH {
            let _ = unsafe { cursor.read_next_unchecked::<u8>() };
            state.bit_pos = 0;
        }
        *dst = T::from_u64(v);
    }

    Ok(())
}

/// Optimized unpack for aligned reads.
#[allow(unused)] // TODO: Will put in.
fn bit_unpack_aligned<T>(
    state: &BitUnpackState,
    cursor: &mut ReadCursor,
    out: &mut [T],
) -> Result<()>
where
    T: BitPackEncodeable,
{
    let mask = BITPACK_MASKS[state.bit_width as usize];

    for dst in out {
        // Read the first byte and extract bits.
        let byte = unsafe { cursor.read_next_unchecked::<u8>() };
        let mut val = (byte as u64) & mask;
        let mut local_pos = state.bit_width; // local bit position within the value
        // If the value spans multiple bytes:
        while local_pos > BYTE_WIDTH {
            let next = unsafe { cursor.read_next_unchecked::<u8>() };
            let shift_amount = BYTE_WIDTH - (local_pos - state.bit_width);
            val |= ((next as u64) << shift_amount) & mask;
            local_pos -= BYTE_WIDTH;
        }
        *dst = T::from_u64(val);
    }

    Ok(())
}

/// Reads an unsigned vlq from the cursor.
///
/// The most-significant bit acts as a continuation flag; the lower 7 bits are
/// accumulated into the result in little-endian order.
pub fn read_unsigned_vlq(cursor: &mut ReadCursor) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u8;
    loop {
        let byte = unsafe { cursor.read_next_unchecked::<u8>() };
        result |= ((byte & 0x7F) as u64) << shift;
        // If the continuation bit is not set, we're done.
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(DbError::new("VLQ integer too large"));
        }
    }
    Ok(result)
}

/// BitPackEncodeable trait allows converting from a u64 to the target type.
/// Only the least-significant bytes that fit in the type will be used.
pub trait BitPackEncodeable: Copy {
    /// Convert a u64 to Self.
    fn from_u64(v: u64) -> Self;
}

macro_rules! impl_rle_encodeable {
    ($native:ty) => {
        impl BitPackEncodeable for $native {
            fn from_u64(v: u64) -> Self {
                // Only the LSB bytes that fit in this type are relevant.
                let bs = &v.to_le_bytes()[..std::mem::size_of::<Self>()];
                Self::from_le_bytes(bs.try_into().unwrap())
            }
        }
    };
}

impl_rle_encodeable!(u8);
impl_rle_encodeable!(u16);
impl_rle_encodeable!(u32);
impl_rle_encodeable!(u64);
impl_rle_encodeable!(i8);
impl_rle_encodeable!(i16);
impl_rle_encodeable!(i32);
impl_rle_encodeable!(i64);

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use glaredb_core::buffer::buffer_manager::NopBufferManager;

    use super::*;
    use crate::column::read_buffer::OwnedReadBuffer;

    #[test]
    fn bit_unpacker_u8_bit_width_1() {
        // 1-bit unpacking
        //
        // [1, 1, 0, 0, 1, 1, 0, 1]
        let raw = [0b10110011];
        let mut buf = OwnedReadBuffer::from_bytes(&NopBufferManager, raw).unwrap();
        let mut cursor = buf.take_remaining();
        let mut out = [0u8; 8];
        let mut state = BitUnpackState::new(1);
        bit_unpack::<u8>(&mut state, &mut cursor, &mut out).unwrap();
        assert_eq!(out, [1, 1, 0, 0, 1, 1, 0, 1]);
    }

    #[test]
    fn bit_unpacker_u8_bit_width_3() {
        // 3-bit unpacking, [5, 2, 7]
        // - 5 => 101 (bits 0–2)
        // - 2 => 010 (bits 3–5)
        // - 7 => 111 (bits 6–8, spanning into the second byte)
        let raw = [0b11010101, 0b00000001];
        let mut buf = OwnedReadBuffer::from_bytes(&NopBufferManager, raw).unwrap();
        let mut cursor = buf.take_remaining();
        let mut out = [0u8; 3];
        let mut state = BitUnpackState::new(3);
        bit_unpack::<u8>(&mut state, &mut cursor, &mut out).unwrap();
        assert_eq!(out, [5, 2, 7]);
    }

    #[test]
    fn bit_unpacker_u16_bit_width_9() {
        // 9-bit unpacking into u16 values.

        let values: [u16; 3] = [0x1FF, 0x100, 0x0AB];
        let width = 9;
        // Pack the values into a u64 accumulator in little–endian order.
        let mut acc: u64 = 0;
        let mut bits = 0;
        for &v in &values {
            acc |= (v as u64) << bits;
            bits += width;
        }
        // Determine the required byte length.
        let byte_len = (bits + 7) / 8;
        let mut raw = vec![0u8; byte_len];
        for i in 0..byte_len {
            raw[i] = ((acc >> (i * 8)) & 0xFF) as u8;
        }
        let mut buf = OwnedReadBuffer::from_bytes(&NopBufferManager, raw).unwrap();
        let mut cursor = buf.take_remaining();
        let mut out = [0u16; 3];
        let mut state = BitUnpackState::new(width as u8);
        bit_unpack::<u16>(&mut state, &mut cursor, &mut out).unwrap();
        assert_eq!(out, values);
    }

    #[test]
    fn read_unsigned_vlq_basic() {
        // The value 300 (0b1 0010 1100) should be encoded as two bytes:
        // 0b10101100 (0xAC) and 0b00000010 (0x02)
        //
        // The value 127 (0x7F) as a single byte.
        let raw = [0xAC, 0x02, 0x7F];
        let mut buf = OwnedReadBuffer::from_bytes(&NopBufferManager, raw).unwrap();
        let mut cursor = buf.take_remaining();
        let vlq1 = read_unsigned_vlq(&mut cursor).expect("read vlq failed");
        let vlq2 = read_unsigned_vlq(&mut cursor).expect("read vlq failed");
        assert_eq!(vlq1, 300);
        assert_eq!(vlq2, 127);
    }
}
