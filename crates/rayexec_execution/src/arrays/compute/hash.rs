use ahash::RandomState;
use half::f16;
use rayexec_error::{not_implemented, Result};
use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::array::physical_type::{
    Addressable,
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalStorage,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
    UntypedNull,
};
use crate::arrays::array::Array;
use crate::arrays::scalar::interval::Interval;

/// State used for all hashing operations during physical execution.
pub const HASH_RANDOM_STATE: RandomState = RandomState::with_seeds(0, 0, 0, 0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DefaultHasher;

impl DefaultHasher {
    const NULL_HASH: u64 = 0xA21258D088C87A13;

    fn hash<V: HashValue + ?Sized>(val: &V) -> u64 {
        val.hash_one()
    }

    /// Combine two hashes into a single value.
    ///
    /// Implementation taken from boost:
    /// <https://github.com/boostorg/container_hash/blob/b8179488b20eb1373bdbf5c7fcca963f072512df/include/boost/container_hash/detail/hash_mix.hpp#L67>
    const fn combine_hashes(v1: u64, v2: u64) -> u64 {
        const fn mix(mut x: u64) -> u64 {
            const M: u64 = 0xE9846AF9B1A615D;
            x ^= x.wrapping_shr(32);
            x = x.wrapping_mul(M);
            x ^= x.wrapping_shr(32);
            x = x.wrapping_mul(M);
            x ^= x.wrapping_shr(28);
            x
        }

        mix(v1 + 0x9E3779B9 + v2)
    }
}

/// Hash an array selection, writing the hash values to `hashes`.
///
/// Length of selection and hashes slice must be the same.
pub fn hash_array(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    hashes: &mut [u64],
) -> Result<()> {
    match arr.datatype().physical_type() {
        PhysicalType::UntypedNull => hash_array_inner::<PhysicalUntypedNull>(arr, sel, hashes),
        PhysicalType::Boolean => hash_array_inner::<PhysicalBool>(arr, sel, hashes),
        PhysicalType::Int8 => hash_array_inner::<PhysicalI8>(arr, sel, hashes),
        PhysicalType::Int16 => hash_array_inner::<PhysicalI16>(arr, sel, hashes),
        PhysicalType::Int32 => hash_array_inner::<PhysicalI32>(arr, sel, hashes),
        PhysicalType::Int64 => hash_array_inner::<PhysicalI64>(arr, sel, hashes),
        PhysicalType::Int128 => hash_array_inner::<PhysicalI128>(arr, sel, hashes),
        PhysicalType::UInt8 => hash_array_inner::<PhysicalU8>(arr, sel, hashes),
        PhysicalType::UInt16 => hash_array_inner::<PhysicalU16>(arr, sel, hashes),
        PhysicalType::UInt32 => hash_array_inner::<PhysicalU32>(arr, sel, hashes),
        PhysicalType::UInt64 => hash_array_inner::<PhysicalU64>(arr, sel, hashes),
        PhysicalType::UInt128 => hash_array_inner::<PhysicalU128>(arr, sel, hashes),
        PhysicalType::Float16 => hash_array_inner::<PhysicalF16>(arr, sel, hashes),
        PhysicalType::Float32 => hash_array_inner::<PhysicalF32>(arr, sel, hashes),
        PhysicalType::Float64 => hash_array_inner::<PhysicalF64>(arr, sel, hashes),
        PhysicalType::Interval => hash_array_inner::<PhysicalInterval>(arr, sel, hashes),
        PhysicalType::Utf8 => hash_array_inner::<PhysicalUtf8>(arr, sel, hashes),
        PhysicalType::Binary => hash_array_inner::<PhysicalBinary>(arr, sel, hashes),

        other => not_implemented!("hash physical type: {other:?}"),
    }
}

fn hash_array_inner<S>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    hashes: &mut [u64],
) -> Result<()>
where
    S: PhysicalStorage,
    S::StorageType: HashValue,
{
    let sel = sel.into_iter();
    debug_assert_eq!(sel.len(), hashes.len());

    let arr = arr.flat_view()?;
    let values = S::get_addressable(&arr.array_buffer)?;

    if arr.validity.all_valid() {
        for (idx, hash) in sel.zip(hashes.iter_mut()) {
            let sel_idx = arr.selection.get(idx).unwrap();
            let v = values.get(sel_idx).unwrap();
            *hash = DefaultHasher::hash(v);
        }
    } else {
        for (idx, hash) in sel.zip(hashes.iter_mut()) {
            let sel_idx = arr.selection.get(idx).unwrap();
            if arr.validity.is_valid(sel_idx) {
                let v = values.get(sel_idx).unwrap();
                *hash = DefaultHasher::hash(v);
            } else {
                *hash = DefaultHasher::NULL_HASH;
            }
        }
    }

    Ok(())
}

/// Helper trait for hashing values.
///
/// This is mostly for floats since they don't automatically implement `Hash`.
trait HashValue {
    fn hash_one(&self) -> u64;
}

macro_rules! impl_hash_value {
    ($typ:ty) => {
        impl HashValue for $typ {
            fn hash_one(&self) -> u64 {
                HASH_RANDOM_STATE.hash_one(self)
            }
        }
    };
}

impl_hash_value!(bool);
impl_hash_value!(i8);
impl_hash_value!(i16);
impl_hash_value!(i32);
impl_hash_value!(i64);
impl_hash_value!(i128);
impl_hash_value!(u8);
impl_hash_value!(u16);
impl_hash_value!(u32);
impl_hash_value!(u64);
impl_hash_value!(u128);
impl_hash_value!(str);
impl_hash_value!([u8]);
impl_hash_value!(Interval);

impl HashValue for f16 {
    fn hash_one(&self) -> u64 {
        HASH_RANDOM_STATE.hash_one(self.to_ne_bytes())
    }
}

impl HashValue for f32 {
    fn hash_one(&self) -> u64 {
        HASH_RANDOM_STATE.hash_one(self.to_ne_bytes())
    }
}

impl HashValue for f64 {
    fn hash_one(&self) -> u64 {
        HASH_RANDOM_STATE.hash_one(self.to_ne_bytes())
    }
}

impl HashValue for UntypedNull {
    fn hash_one(&self) -> u64 {
        DefaultHasher::NULL_HASH
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;

    #[test]
    fn combine_hashes_not_zero() {
        let out = DefaultHasher::combine_hashes(0, 0);
        assert_ne!(0, out);
    }

    #[test]
    fn hash_i32_with_invalid() {
        let mut hashes = vec![0; 4];
        let arr = Array::try_from_iter([Some(1), Some(2), None, Some(4)]).unwrap();

        hash_array(&arr, 0..4, &mut hashes).unwrap();

        assert_eq!(DefaultHasher::NULL_HASH, hashes[2]);
    }

    #[test]
    fn hash_i32_dictionary() {
        let mut hashes = vec![0; 4];
        let mut arr = Array::try_from_iter([2, 3]).unwrap();
        arr.select(&Arc::new(NopBufferManager), [0, 1, 0, 1])
            .unwrap();

        hash_array(&arr, 0..4, &mut hashes).unwrap();

        assert_eq!(hashes[0], hashes[2]);
        assert_eq!(hashes[1], hashes[3]);
    }
}
