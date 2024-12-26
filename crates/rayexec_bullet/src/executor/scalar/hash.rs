use ahash::RandomState;
use half::f16;
use rayexec_error::{RayexecError, Result};

use crate::array::{ArrayData, ArrayOld};
use crate::executor::physical_type::{
    PhysicalBinaryOld,
    PhysicalBoolOld,
    PhysicalF16Old,
    PhysicalF32Old,
    PhysicalF64Old,
    PhysicalI128Old,
    PhysicalI16Old,
    PhysicalI32Old,
    PhysicalI64Old,
    PhysicalI8Old,
    PhysicalIntervalOld,
    PhysicalList,
    PhysicalStorageOld,
    PhysicalType,
    PhysicalU16Old,
    PhysicalU32Old,
    PhysicalU64Old,
    PhysicalU8Old,
    PhysicalUntypedNullOld,
    PhysicalUtf8Old,
};
use crate::scalar::interval::Interval;
use crate::selection;
use crate::storage::{AddressableStorage, UntypedNull};

/// State used for all hashing operations during physical execution.
pub const HASH_RANDOM_STATE: RandomState = RandomState::with_seeds(0, 0, 0, 0);

#[derive(Debug, Clone)]
pub struct HashExecutor;

impl HashExecutor {
    /// Hashes the given array values, combining them with the existing hashes
    /// in `hashes`.
    pub fn hash_combine(array: &ArrayOld, hashes: &mut [u64]) -> Result<()> {
        match array.physical_type() {
            PhysicalType::UntypedNull => {
                Self::hash_one_inner::<PhysicalUntypedNullOld, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Boolean => {
                Self::hash_one_inner::<PhysicalBoolOld, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Int8 => {
                Self::hash_one_inner::<PhysicalI8Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Int16 => {
                Self::hash_one_inner::<PhysicalI16Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Int32 => {
                Self::hash_one_inner::<PhysicalI32Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Int64 => {
                Self::hash_one_inner::<PhysicalI64Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Int128 => {
                Self::hash_one_inner::<PhysicalI128Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::UInt8 => {
                Self::hash_one_inner::<PhysicalU8Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::UInt16 => {
                Self::hash_one_inner::<PhysicalU16Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::UInt32 => {
                Self::hash_one_inner::<PhysicalU32Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::UInt64 => {
                Self::hash_one_inner::<PhysicalU64Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::UInt128 => {
                Self::hash_one_inner::<PhysicalI128Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Float16 => {
                Self::hash_one_inner::<PhysicalF16Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Float32 => {
                Self::hash_one_inner::<PhysicalF32Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Float64 => {
                Self::hash_one_inner::<PhysicalF64Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Binary => {
                Self::hash_one_inner::<PhysicalBinaryOld, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Utf8 => {
                Self::hash_one_inner::<PhysicalUtf8Old, CombineSetHash>(array, hashes)?
            }
            PhysicalType::Interval => {
                Self::hash_one_inner::<PhysicalIntervalOld, CombineSetHash>(array, hashes)?
            }
            PhysicalType::List => Self::hash_list::<CombineSetHash>(array, hashes)?,
        }

        Ok(())
    }

    /// Hash the given array and write the values into `hashes`, overwriting any
    /// existing values.
    pub fn hash_no_combine(array: &ArrayOld, hashes: &mut [u64]) -> Result<()> {
        match array.physical_type() {
            PhysicalType::UntypedNull => {
                Self::hash_one_inner::<PhysicalUntypedNullOld, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Boolean => {
                Self::hash_one_inner::<PhysicalBoolOld, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Int8 => {
                Self::hash_one_inner::<PhysicalI8Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Int16 => {
                Self::hash_one_inner::<PhysicalI16Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Int32 => {
                Self::hash_one_inner::<PhysicalI32Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Int64 => {
                Self::hash_one_inner::<PhysicalI64Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Int128 => {
                Self::hash_one_inner::<PhysicalI128Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::UInt8 => {
                Self::hash_one_inner::<PhysicalU8Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::UInt16 => {
                Self::hash_one_inner::<PhysicalU16Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::UInt32 => {
                Self::hash_one_inner::<PhysicalU32Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::UInt64 => {
                Self::hash_one_inner::<PhysicalU64Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::UInt128 => {
                Self::hash_one_inner::<PhysicalI128Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Float16 => {
                Self::hash_one_inner::<PhysicalF16Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Float32 => {
                Self::hash_one_inner::<PhysicalF32Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Float64 => {
                Self::hash_one_inner::<PhysicalF64Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Binary => {
                Self::hash_one_inner::<PhysicalBinaryOld, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Utf8 => {
                Self::hash_one_inner::<PhysicalUtf8Old, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::Interval => {
                Self::hash_one_inner::<PhysicalIntervalOld, OverwriteSetHash>(array, hashes)?
            }
            PhysicalType::List => Self::hash_list::<OverwriteSetHash>(array, hashes)?,
        }

        Ok(())
    }

    pub fn hash_many<'b>(arrays: &[ArrayOld], hashes: &'b mut [u64]) -> Result<&'b mut [u64]> {
        for (idx, array) in arrays.iter().enumerate() {
            let combine_hash = idx > 0;

            if combine_hash {
                Self::hash_combine(array, hashes)?;
            } else {
                Self::hash_no_combine(array, hashes)?;
            }
        }

        Ok(hashes)
    }

    fn hash_one_inner<'a, 'b, S, H>(array: &'a ArrayOld, hashes: &'b mut [u64]) -> Result<()>
    where
        S: PhysicalStorageOld,
        S::Type<'a>: HashValue,
        H: SetHash,
    {
        let selection = array.selection_vector();

        match array.validity() {
            Some(validity) => {
                let values = S::get_storage(&array.data)?;

                for (idx, hash) in hashes.iter_mut().enumerate() {
                    let sel = unsafe { selection::get_unchecked(selection, idx) };

                    if validity.value(sel) {
                        let val = unsafe { values.get_unchecked(sel) };
                        H::set_hash(val.hash_one(), hash);
                    } else {
                        H::set_hash(null_hash_value(), hash)
                    }
                }
            }
            None => {
                let values = S::get_storage(&array.data)?;

                for (idx, hash) in hashes.iter_mut().enumerate() {
                    let sel = unsafe { selection::get_unchecked(selection, idx) };
                    let val = unsafe { values.get_unchecked(sel) };
                    H::set_hash(val.hash_one(), hash);
                }
            }
        }

        Ok(())
    }

    fn hash_list<H>(array: &ArrayOld, hashes: &mut [u64]) -> Result<()>
    where
        H: SetHash,
    {
        let inner = match array.array_data() {
            ArrayData::List(list) => &list.array,
            other => {
                return Err(RayexecError::new(format!(
                    "Unexpected array data for list hashing: {:?}",
                    other.physical_type(),
                )))
            }
        };

        // TODO: Try to avoid this.
        let mut list_hashes_buf = vec![0; inner.logical_len()];
        Self::hash_no_combine(inner, &mut list_hashes_buf)?;

        let metadata = PhysicalList::get_storage(&array.data)?;
        let selection = array.selection_vector();

        match array.validity() {
            Some(validity) => {
                for (idx, hash) in hashes.iter_mut().enumerate() {
                    let sel = unsafe { selection::get_unchecked(selection, idx) };

                    if validity.value(sel) {
                        let val = unsafe { metadata.get_unchecked(sel) };

                        // Set first hash.
                        H::set_hash(list_hashes_buf[val.offset as usize], hash);

                        // Combine all the rest.
                        for hash_idx in 1..val.len {
                            CombineSetHash::set_hash(
                                list_hashes_buf[(val.offset + hash_idx) as usize],
                                hash,
                            );
                        }
                    } else {
                        H::set_hash(null_hash_value(), hash);
                    }
                }
            }
            None => {
                for (idx, hash) in hashes.iter_mut().enumerate() {
                    let sel = unsafe { selection::get_unchecked(selection, idx) };
                    let val = unsafe { metadata.get_unchecked(sel) };

                    // Set first hash.
                    H::set_hash(list_hashes_buf[val.offset as usize], hash);

                    // Combine all the rest.
                    for hash_idx in 1..val.len {
                        CombineSetHash::set_hash(
                            list_hashes_buf[(val.offset + hash_idx) as usize],
                            hash,
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

trait SetHash {
    fn set_hash(new_hash_value: u64, existing: &mut u64);
}

#[derive(Debug, Clone, Copy)]
struct OverwriteSetHash;

impl SetHash for OverwriteSetHash {
    fn set_hash(new_hash_value: u64, existing: &mut u64) {
        *existing = new_hash_value
    }
}

#[derive(Debug, Clone, Copy)]
struct CombineSetHash;

impl SetHash for CombineSetHash {
    fn set_hash(new_hash_value: u64, existing: &mut u64) {
        *existing = combine_hashes(new_hash_value, *existing)
    }
}

/// All nulls should hash to the same value.
///
/// _What_ that value is is arbitrary, but it needs to be consistent.
fn null_hash_value() -> u64 {
    HASH_RANDOM_STATE.hash_one(1)
}

/// Combines two hashes into one hash
///
/// This implementation came from datafusion.
const fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
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
impl_hash_value!(&str);
impl_hash_value!(&[u8]);
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
        null_hash_value()
    }
}
