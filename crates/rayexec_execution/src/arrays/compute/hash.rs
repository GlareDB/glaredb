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
    PhysicalList,
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
use crate::arrays::array::selection::Selection;
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

        mix(v1.wrapping_add(0x9E3779B9).wrapping_add(v2))
    }
}

/// Determines how we set a hash.
trait SetHashOp {
    fn set_hash(current: &mut u64, new: u64);
}

/// Overwrites the hash value.
#[derive(Debug, Clone, Copy)]
struct OverwriteHash;

impl SetHashOp for OverwriteHash {
    fn set_hash(current: &mut u64, new: u64) {
        *current = new
    }
}

/// Combines the hash of a value with the existing hash.
#[derive(Debug, Clone, Copy)]
struct CombineHash;

impl SetHashOp for CombineHash {
    fn set_hash(current: &mut u64, new: u64) {
        *current = DefaultHasher::combine_hashes(*current, new)
    }
}

/// Hash an array selection, writing the hash values to `hashes`.
///
/// Length of selection and hashes slice must be the same.
///
/// This will overwrite hashes.
pub fn hash_array(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    hashes: &mut [u64],
) -> Result<()> {
    hash_inner::<OverwriteHash>(arr, sel, hashes)
}

/// Hashes multiple arrays at once, combining each row's hash into a single hash
/// value.
///
/// Length of selection and hashes slice must be the same. The same selection is
/// applied to every input array.
pub fn hash_many_arrays<'a>(
    arrs: impl IntoIterator<Item = &'a Array>,
    sel: Selection,
    hashes: &mut [u64],
) -> Result<()> {
    for (idx, arr) in arrs.into_iter().enumerate() {
        if idx == 0 {
            hash_inner::<OverwriteHash>(arr, sel, hashes)?;
        } else {
            hash_inner::<CombineHash>(arr, sel, hashes)?;
        }
    }

    Ok(())
}

fn hash_inner<H>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    hashes: &mut [u64],
) -> Result<()>
where
    H: SetHashOp,
{
    match arr.datatype().physical_type() {
        PhysicalType::UntypedNull => hash_typed_inner::<PhysicalUntypedNull, H>(arr, sel, hashes),
        PhysicalType::Boolean => hash_typed_inner::<PhysicalBool, H>(arr, sel, hashes),
        PhysicalType::Int8 => hash_typed_inner::<PhysicalI8, H>(arr, sel, hashes),
        PhysicalType::Int16 => hash_typed_inner::<PhysicalI16, H>(arr, sel, hashes),
        PhysicalType::Int32 => hash_typed_inner::<PhysicalI32, H>(arr, sel, hashes),
        PhysicalType::Int64 => hash_typed_inner::<PhysicalI64, H>(arr, sel, hashes),
        PhysicalType::Int128 => hash_typed_inner::<PhysicalI128, H>(arr, sel, hashes),
        PhysicalType::UInt8 => hash_typed_inner::<PhysicalU8, H>(arr, sel, hashes),
        PhysicalType::UInt16 => hash_typed_inner::<PhysicalU16, H>(arr, sel, hashes),
        PhysicalType::UInt32 => hash_typed_inner::<PhysicalU32, H>(arr, sel, hashes),
        PhysicalType::UInt64 => hash_typed_inner::<PhysicalU64, H>(arr, sel, hashes),
        PhysicalType::UInt128 => hash_typed_inner::<PhysicalU128, H>(arr, sel, hashes),
        PhysicalType::Float16 => hash_typed_inner::<PhysicalF16, H>(arr, sel, hashes),
        PhysicalType::Float32 => hash_typed_inner::<PhysicalF32, H>(arr, sel, hashes),
        PhysicalType::Float64 => hash_typed_inner::<PhysicalF64, H>(arr, sel, hashes),
        PhysicalType::Interval => hash_typed_inner::<PhysicalInterval, H>(arr, sel, hashes),
        PhysicalType::Utf8 => hash_typed_inner::<PhysicalUtf8, H>(arr, sel, hashes),
        PhysicalType::Binary => hash_typed_inner::<PhysicalBinary, H>(arr, sel, hashes),
        PhysicalType::List => hash_list_array::<H>(arr, sel, hashes),

        other => not_implemented!("hash physical type: {other:?}"),
    }
}

fn hash_typed_inner<S, H>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    hashes: &mut [u64],
) -> Result<()>
where
    S: PhysicalStorage,
    S::StorageType: HashValue,
    H: SetHashOp,
{
    let sel = sel.into_iter();
    debug_assert_eq!(sel.len(), hashes.len());

    let arr = arr.flat_view()?;
    let values = S::get_addressable(&arr.array_buffer)?;

    if arr.validity.all_valid() {
        for (idx, hash) in sel.zip(hashes.iter_mut()) {
            let sel_idx = arr.selection.get(idx).unwrap();
            let v = values.get(sel_idx).unwrap();
            H::set_hash(hash, DefaultHasher::hash(v));
        }
    } else {
        for (idx, hash) in sel.zip(hashes.iter_mut()) {
            let sel_idx = arr.selection.get(idx).unwrap();
            if arr.validity.is_valid(sel_idx) {
                let v = values.get(sel_idx).unwrap();
                H::set_hash(hash, DefaultHasher::hash(v));
            } else {
                H::set_hash(hash, DefaultHasher::NULL_HASH);
            }
        }
    }

    Ok(())
}

fn hash_list_array<H>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    hashes: &mut [u64],
) -> Result<()>
where
    H: SetHashOp,
{
    let arr = arr.flat_view()?;

    let list_buf = arr.array_buffer.get_secondary().get_list()?;
    let metadatas = PhysicalList::get_addressable(&arr.array_buffer)?;

    // TODO: Would be cool not having to allocate here.
    let mut child_hashes = Vec::new();

    for (idx, hash) in sel.into_iter().zip(hashes.iter_mut()) {
        let sel_idx = arr.selection.get(idx).unwrap();

        if arr.validity.is_valid(sel_idx) {
            let meta = metadatas.get(sel_idx).unwrap();

            child_hashes.clear();
            child_hashes.resize(meta.len as usize, 0);

            let sel = Selection::linear(meta.offset as usize, meta.len as usize);
            hash_inner::<H>(&list_buf.child, sel, &mut child_hashes)?;

            // Now combine all the child hashes into one.
            let mut child_hash = match child_hashes.first() {
                Some(hash) => *hash,
                None => DefaultHasher::NULL_HASH, // Default to null hash if working with empty list.
            };

            for &hash2 in &child_hashes {
                child_hash = DefaultHasher::combine_hashes(child_hash, hash2);
            }

            // Set main hash.
            H::set_hash(hash, child_hash);
        } else {
            H::set_hash(hash, DefaultHasher::NULL_HASH);
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
    use crate::arrays::compute::make_list::make_list_from_values;
    use crate::arrays::datatype::{DataType, ListTypeMeta};

    #[test]
    fn combine_hashes_not_zero() {
        let out = DefaultHasher::combine_hashes(0, 0);
        assert_ne!(0, out);
    }

    #[test]
    fn hash_i32() {
        // Make sure we overwrite the hash values if just hashing a single
        // array.
        let mut hashes = vec![1, 2];
        let arr = Array::try_from_iter([4, 5]).unwrap();

        hash_array(&arr, 0..2, &mut hashes).unwrap();

        assert_ne!(1, hashes[0]);
        assert_ne!(2, hashes[1]);
    }

    #[test]
    fn hash_i32_with_invalid() {
        let mut hashes = vec![0; 4];
        let arr = Array::try_from_iter([Some(1), Some(2), None, Some(4)]).unwrap();

        hash_array(&arr, 0..4, &mut hashes).unwrap();

        assert_eq!(DefaultHasher::NULL_HASH, hashes[2]);
    }

    #[test]
    fn hash_with_selection() {
        let mut hashes = vec![0; 2];
        let arr = Array::try_from_iter([Some(1), Some(2), None, Some(4)]).unwrap();

        hash_array(&arr, [0, 2], &mut hashes).unwrap();

        assert_ne!(0, hashes[0]);
        assert_eq!(DefaultHasher::NULL_HASH, hashes[1]);
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

    #[test]
    fn hash_i32_lists() {
        let mut hashes = vec![0; 4];

        let mut lists = Array::try_new(
            &Arc::new(NopBufferManager),
            DataType::List(ListTypeMeta::new(DataType::Int32)),
            4,
        )
        .unwrap();
        // Rows 0 and 2 have the same list values.
        make_list_from_values(
            &[
                Array::try_from_iter([1, 2, 1, 4]).unwrap(),
                Array::try_from_iter([5, 6, 5, 8]).unwrap(),
                Array::try_from_iter([9, 10, 9, 12]).unwrap(),
            ],
            0..4,
            &mut lists,
        )
        .unwrap();

        hash_array(&lists, 0..4, &mut hashes).unwrap();

        assert_ne!(0, hashes[0]);
        assert_eq!(hashes[0], hashes[2]);
    }

    #[test]
    fn hash_many_i32() {
        let arrs = [
            Array::try_from_iter([1, 2]).unwrap(),
            Array::try_from_iter([2, 2]).unwrap(),
        ];

        let mut hashes = vec![0; 2];

        hash_many_arrays(&arrs, Selection::linear(0, 2), &mut hashes).unwrap();
        assert_ne!(hashes[0], hashes[1]);
    }
}
