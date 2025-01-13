use half::f16;
use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::flat::FlatArrayView;
use crate::arrays::array::physical_type::{
    Addressable,
    PhysicalI32,
    PhysicalI8,
    PhysicalStorage,
    PhysicalType,
};
use crate::arrays::array::Array;
use crate::arrays::scalar::interval::Interval;

/// Encodes a selection of rows into an output buffer.
///
/// `buf` is guaranteed to fit exactly the number of bytes needed for encoding.
pub fn prefix_encode<B>(
    array: &Array<B>,
    selection: impl IntoExactSizeIterator<Item = usize>,
    add_offset: usize,
    offsets: &[usize],
    buf: &mut [u8],
    nulls_first: bool,
    desc: bool,
) -> Result<()>
where
    B: BufferManager,
{
    let flat = array.flat_view()?;
    match flat.array_buffer.physical_type() {
        PhysicalType::Int8 => encode_fixedlen::<PhysicalI8, _>(
            flat,
            selection,
            add_offset,
            offsets,
            buf,
            nulls_first,
            desc,
        ),
        PhysicalType::Int32 => encode_fixedlen::<PhysicalI32, _>(
            flat,
            selection,
            add_offset,
            offsets,
            buf,
            nulls_first,
            desc,
        ),
        _ => unimplemented!(),
    }
}

pub fn encode_fixedlen<S, B>(
    flat: FlatArrayView<B>,
    selection: impl IntoExactSizeIterator<Item = usize>,
    add_offset: usize,
    offsets: &[usize],
    buf: &mut [u8],
    nulls_first: bool,
    desc: bool,
) -> Result<()>
where
    S: PhysicalStorage,
    S::StorageType: ComparableEncode + Sized,
    B: BufferManager,
{
    let input = S::get_addressable(&flat.array_buffer)?;

    let valid_byte: u8 = if nulls_first { 1 } else { 0 };
    let invalid_byte: u8 = 1 - valid_byte;

    if flat.validity.all_valid() {
        for (output_idx, input_idx) in selection.into_iter().enumerate() {
            let offset = offsets[output_idx] + add_offset;
            buf[offset] = valid_byte;
            let offset = offset + 1;

            let v = input.get(input_idx).unwrap();

            let buf = &mut buf[offset..offset + std::mem::size_of::<S::StorageType>()];
            v.encode(buf);

            // Flip bytes.
            if desc {
                for byte in buf {
                    *byte = !*byte;
                }
            }
        }
    } else {
        for (output_idx, input_idx) in selection.into_iter().enumerate() {
            let offset = offsets[output_idx] + add_offset;

            if flat.validity.is_valid(input_idx) {
                buf[offset] = valid_byte;
                let offset = offset + 1;

                let v = input.get(input_idx).unwrap();

                let buf = &mut buf[offset..offset + std::mem::size_of::<S::StorageType>()];
                v.encode(buf);

                // Flip bytes.
                if desc {
                    for byte in buf {
                        *byte = !*byte;
                    }
                }
            } else {
                buf[offset] = invalid_byte;
                let offset = offset + 1;

                // Reset buf.
                let buf = &mut buf[offset..offset + std::mem::size_of::<S::StorageType>()];
                for byte in buf {
                    *byte = 0;
                }
            }
        }
    }

    Ok(())
}

/// Trait for types that can encode themselves into a comparable binary
/// representation.
pub trait ComparableEncode {
    fn encode(&self, buf: &mut [u8]);
}

/// Implements `ComparableEncode` for unsigned ints.
macro_rules! comparable_encode_unsigned {
    ($type:ty) => {
        impl ComparableEncode for $type {
            fn encode(&self, buf: &mut [u8]) {
                let b = self.to_be_bytes();
                buf.copy_from_slice(&b);
            }
        }
    };
}

comparable_encode_unsigned!(u8);
comparable_encode_unsigned!(u16);
comparable_encode_unsigned!(u32);
comparable_encode_unsigned!(u64);
comparable_encode_unsigned!(u128);

/// Implements `ComparableEncode` for signed ints.
macro_rules! comparable_encode_signed {
    ($type:ty) => {
        impl ComparableEncode for $type {
            fn encode(&self, buf: &mut [u8]) {
                let mut b = self.to_be_bytes();
                b[0] ^= 128; // Flip sign bit.
                buf.copy_from_slice(&b);
            }
        }
    };
}

comparable_encode_signed!(i8);
comparable_encode_signed!(i16);
comparable_encode_signed!(i32);
comparable_encode_signed!(i64);
comparable_encode_signed!(i128);

impl ComparableEncode for f16 {
    fn encode(&self, buf: &mut [u8]) {
        let bits = self.to_bits() as i16;
        let v = bits ^ (((bits >> 15) as u16) >> 1) as i16;
        v.encode(buf)
    }
}

impl ComparableEncode for f32 {
    fn encode(&self, buf: &mut [u8]) {
        // Adapted from <https://github.com/rust-lang/rust/blob/791adf759cc065316f054961875052d5bc03e16c/library/core/src/num/f32.rs#L1456-L1485>
        let bits = self.to_bits() as i32;
        let v = bits ^ (((bits >> 31) as u32) >> 1) as i32;
        v.encode(buf)
    }
}

impl ComparableEncode for f64 {
    fn encode(&self, buf: &mut [u8]) {
        // Adapted from <https://github.com/rust-lang/rust/blob/791adf759cc065316f054961875052d5bc03e16c/library/core/src/num/f32.rs#L1456-L1485>
        let bits = self.to_bits() as i64;
        let v = bits ^ (((bits >> 31) as u64) >> 1) as i64;
        v.encode(buf)
    }
}

impl ComparableEncode for Interval {
    fn encode(&self, buf: &mut [u8]) {
        // TODO: We'll probably need to ensure intervals are normalized.
        self.months.encode(buf);
        self.days.encode(buf);
        self.nanos.encode(buf);
    }
}

// FALSE < TRUE
impl ComparableEncode for bool {
    fn encode(&self, buf: &mut [u8]) {
        if *self {
            buf[0] = 0;
        } else {
            buf[0] = 255;
        }
    }
}

impl ComparableEncode for &str {
    fn encode(&self, buf: &mut [u8]) {
        buf.copy_from_slice(self.as_bytes())
    }
}

impl ComparableEncode for &[u8] {
    fn encode(&self, buf: &mut [u8]) {
        buf.copy_from_slice(self)
    }
}
