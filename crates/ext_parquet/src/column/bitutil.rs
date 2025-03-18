use glaredb_error::{DbError, Result};

use super::read_buffer::ReadBuffer;

/// All possible masks for an 8-byte wide value.
///
/// [0, 1, 3, 7, 15, ..., u64::MAX]
pub const BITPACK_MASKS: [u64; 65] = {
    let mut masks = [0; 65];
    let mut i = 0;
    while i < 64 {
        masks[i] = (1 << i) - 1;
        i += 1;
    }
    masks[64] = u64::MAX;
    masks
};

pub const BYTE_WIDTH: u8 = 8;

pub const MAX_VLQ_BYTE_LEN_I64: usize = 10;

#[derive(Debug)]
pub struct BitUnpacker<'a> {
    pub buf: &'a mut ReadBuffer,
    pub bit_pos: u8,
    pub bit_width: u8,
}

impl BitUnpacker<'_> {
    pub fn unpack<T>(&mut self, values: &mut [T])
    where
        T: BitPackEncodeable,
    {
        assert!(self.bit_width as usize <= std::mem::size_of::<T>() * 8);

        let mask = BITPACK_MASKS[self.bit_width as usize];

        for value in values {
            // Read value across bytes.
            let mut v =
                (unsafe { self.buf.peek_next_unchecked::<u8>() } as u64 >> self.bit_pos) & mask;
            self.bit_pos += self.bit_width;

            while self.bit_pos > BYTE_WIDTH {
                // Move to next byte.
                unsafe { self.buf.skip_bytes_unchecked(1) };

                let next = unsafe { self.buf.peek_next_unchecked::<u8>() } as u64;
                let shift = (BYTE_WIDTH - (self.bit_pos - self.bit_width)) as u64;

                v |= next << shift;
                v &= mask;

                self.bit_pos -= BYTE_WIDTH;
            }

            *value = T::from_u64(v);
        }
    }

    pub fn read_vlq_i64(&mut self) -> Result<i64> {
        // Ensure we're on a byte boundary.
        if self.bit_pos != 0 {
            self.bit_pos = 0;
            unsafe { self.buf.skip_bytes_unchecked(1) };
        }

        let mut v = 0;
        let mut shift = 0;

        loop {
            // TODO: Length check.
            let b = unsafe { self.buf.read_next_unchecked::<u8>() };
            v |= ((b & 127) as i64) << shift;
            shift += 7;
            // TODO: Max VLQ width check.

            if shift > MAX_VLQ_BYTE_LEN_I64 * 7 {
                return Err(DbError::new("VLQ decoding too large"));
            }

            if b & 128 == 0 {
                return Ok(v);
            }
        }
    }
}

pub trait BitPackEncodeable: Copy {
    /// Convert a u64 to Self.
    ///
    /// When called during unpacking, only the LSB bytes that can fit in this
    /// type will be relevant.
    fn from_u64(v: u64) -> Self;
}

macro_rules! impl_rle_encodeable {
    ($native:ty) => {
        impl BitPackEncodeable for $native {
            fn from_u64(v: u64) -> Self {
                // Trim zero padding. Unpacking guarantees that we're only
                // writing to the relevant parts of the u64.
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

impl BitPackEncodeable for bool {
    fn from_u64(v: u64) -> Self {
        v != 0
    }
}

#[cfg(test)]
mod tests {

    use glaredb_execution::buffer::buffer_manager::NopBufferManager;

    use super::*;
    use crate::column::read_buffer::OwnedReadBuffer;

    #[test]
    fn masks_sanity() {
        assert_eq!(BITPACK_MASKS[0], 0);
        assert_eq!(BITPACK_MASKS[1], 1);
        assert_eq!(BITPACK_MASKS[2], 3);

        assert_eq!(BITPACK_MASKS[7], 127);

        assert_eq!(BITPACK_MASKS[62], 4611686018427387903);
        assert_eq!(BITPACK_MASKS[63], 9223372036854775807);
        assert_eq!(BITPACK_MASKS[64], 18446744073709551615);
    }

    #[test]
    fn bit_unpack_get_aligned_width_3() {
        // 01110101 11001011
        let mut buffer = OwnedReadBuffer::from_bytes(&NopBufferManager, [0x75, 0xCB]).unwrap();
        let mut read = buffer.take_remaining();

        let mut unpacker = BitUnpacker {
            buf: &mut read,
            bit_pos: 0,
            bit_width: 3,
        };

        let mut out = vec![0; 5];
        unpacker.unpack::<i32>(&mut out);

        assert_eq!(&[5, 6, 5, 5, 4], out.as_slice());
    }

    #[test]
    fn bit_unpack_get_aligned_width_8() {
        // 01110101 11001011
        let mut buffer = OwnedReadBuffer::from_bytes(&NopBufferManager, [0x75, 0xCB]).unwrap();
        let mut read = buffer.take_remaining();

        let mut unpacker = BitUnpacker {
            buf: &mut read,
            bit_pos: 0,
            bit_width: 8,
        };

        let mut out = vec![0; 2];
        unpacker.unpack::<i64>(&mut out);

        assert_eq!(&[117, 203], out.as_slice());
    }

    #[test]
    fn bit_unpack_get_aligned_width_13() {
        // 01110101 11001011
        let mut buffer = OwnedReadBuffer::from_bytes(&NopBufferManager, [0x75, 0xCB]).unwrap();
        let mut read = buffer.take_remaining();

        let mut unpacker = BitUnpacker {
            buf: &mut read,
            bit_pos: 0,
            bit_width: 13,
        };

        let mut out = vec![0; 1];
        unpacker.unpack::<i64>(&mut out);

        assert_eq!(&[2933], out.as_slice());
    }

    #[test]
    fn tget_vlq_int() {
        // 10001001 00000001 11110010 10110101 00000110
        let mut buffer =
            OwnedReadBuffer::from_bytes(&NopBufferManager, [0x89, 0x01, 0xF2, 0xB5, 0x06]).unwrap();
        let mut read = buffer.take_remaining();

        let mut unpacker = BitUnpacker {
            buf: &mut read,
            bit_pos: 0,
            bit_width: 3,
        };

        assert_eq!(unpacker.read_vlq_i64().unwrap(), 137);
        assert_eq!(unpacker.read_vlq_i64().unwrap(), 105202);
    }
}
