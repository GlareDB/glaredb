use ahash::RandomState;
use half::f16;
use rayexec_error::Result;

use crate::array::Array;
use crate::executor::physical_type::{
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
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
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
    pub fn hash_combine(array: &Array, hashes: &mut [u64]) -> Result<()> {
        match array.physical_type() {
            PhysicalType::UntypedNull => {
                Self::hash_one_combine::<PhysicalUntypedNull>(array, hashes)?
            }
            PhysicalType::Boolean => Self::hash_one_combine::<PhysicalBool>(array, hashes)?,
            PhysicalType::Int8 => Self::hash_one_combine::<PhysicalI8>(array, hashes)?,
            PhysicalType::Int16 => Self::hash_one_combine::<PhysicalI16>(array, hashes)?,
            PhysicalType::Int32 => Self::hash_one_combine::<PhysicalI32>(array, hashes)?,
            PhysicalType::Int64 => Self::hash_one_combine::<PhysicalI64>(array, hashes)?,
            PhysicalType::Int128 => Self::hash_one_combine::<PhysicalI128>(array, hashes)?,
            PhysicalType::UInt8 => Self::hash_one_combine::<PhysicalU8>(array, hashes)?,
            PhysicalType::UInt16 => Self::hash_one_combine::<PhysicalU16>(array, hashes)?,
            PhysicalType::UInt32 => Self::hash_one_combine::<PhysicalU32>(array, hashes)?,
            PhysicalType::UInt64 => Self::hash_one_combine::<PhysicalU64>(array, hashes)?,
            PhysicalType::UInt128 => Self::hash_one_combine::<PhysicalI128>(array, hashes)?,
            PhysicalType::Float16 => Self::hash_one_combine::<PhysicalF16>(array, hashes)?,
            PhysicalType::Float32 => Self::hash_one_combine::<PhysicalF32>(array, hashes)?,
            PhysicalType::Float64 => Self::hash_one_combine::<PhysicalF64>(array, hashes)?,
            PhysicalType::Binary => Self::hash_one_combine::<PhysicalBinary>(array, hashes)?,
            PhysicalType::Utf8 => Self::hash_one_combine::<PhysicalUtf8>(array, hashes)?,
            PhysicalType::Interval => Self::hash_one_combine::<PhysicalInterval>(array, hashes)?,
        }

        Ok(())
    }

    /// Hash the given array and write the values into `hashes`, overwriting any
    /// existing values.
    pub fn hash_no_combine(array: &Array, hashes: &mut [u64]) -> Result<()> {
        match array.physical_type() {
            PhysicalType::UntypedNull => {
                Self::hash_one_no_combine::<PhysicalUntypedNull>(array, hashes)?
            }
            PhysicalType::Boolean => Self::hash_one_no_combine::<PhysicalBool>(array, hashes)?,
            PhysicalType::Int8 => Self::hash_one_no_combine::<PhysicalI8>(array, hashes)?,
            PhysicalType::Int16 => Self::hash_one_no_combine::<PhysicalI16>(array, hashes)?,
            PhysicalType::Int32 => Self::hash_one_no_combine::<PhysicalI32>(array, hashes)?,
            PhysicalType::Int64 => Self::hash_one_no_combine::<PhysicalI64>(array, hashes)?,
            PhysicalType::Int128 => Self::hash_one_no_combine::<PhysicalI128>(array, hashes)?,
            PhysicalType::UInt8 => Self::hash_one_no_combine::<PhysicalU8>(array, hashes)?,
            PhysicalType::UInt16 => Self::hash_one_no_combine::<PhysicalU16>(array, hashes)?,
            PhysicalType::UInt32 => Self::hash_one_no_combine::<PhysicalU32>(array, hashes)?,
            PhysicalType::UInt64 => Self::hash_one_no_combine::<PhysicalU64>(array, hashes)?,
            PhysicalType::UInt128 => Self::hash_one_no_combine::<PhysicalI128>(array, hashes)?,
            PhysicalType::Float16 => Self::hash_one_no_combine::<PhysicalF16>(array, hashes)?,
            PhysicalType::Float32 => Self::hash_one_no_combine::<PhysicalF32>(array, hashes)?,
            PhysicalType::Float64 => Self::hash_one_no_combine::<PhysicalF64>(array, hashes)?,
            PhysicalType::Binary => Self::hash_one_no_combine::<PhysicalBinary>(array, hashes)?,
            PhysicalType::Utf8 => Self::hash_one_no_combine::<PhysicalUtf8>(array, hashes)?,
            PhysicalType::Interval => Self::hash_one_no_combine::<PhysicalInterval>(array, hashes)?,
        }

        Ok(())
    }

    pub fn hash_many<'b>(arrays: &[Array], hashes: &'b mut [u64]) -> Result<&'b mut [u64]> {
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

    fn hash_one_no_combine<'a, 'b, S>(array: &'a Array, hashes: &'b mut [u64]) -> Result<()>
    where
        S: PhysicalStorage<'a>,
        <S::Storage as AddressableStorage>::T: HashValue,
    {
        let selection = array.selection_vector();

        match array.validity() {
            Some(validity) => {
                let values = S::get_storage(&array.data)?;

                for (idx, hash) in hashes.iter_mut().enumerate() {
                    let sel = unsafe { selection::get_unchecked(selection, idx) };

                    if validity.value(sel) {
                        let val = unsafe { values.get_unchecked(sel) };
                        *hash = val.hash_one();
                    } else {
                        *hash = null_hash_value();
                    }
                }
            }
            None => {
                let values = S::get_storage(&array.data)?;

                for (idx, hash) in hashes.iter_mut().enumerate() {
                    let sel = unsafe { selection::get_unchecked(selection, idx) };
                    let val = unsafe { values.get_unchecked(sel) };
                    *hash = val.hash_one();
                }
            }
        }

        Ok(())
    }

    fn hash_one_combine<'a, 'b, S>(array: &'a Array, hashes: &'b mut [u64]) -> Result<()>
    where
        S: PhysicalStorage<'a>,
        <S::Storage as AddressableStorage>::T: HashValue,
    {
        let selection = array.selection_vector();

        match array.validity() {
            Some(validity) => {
                let values = S::get_storage(&array.data)?;

                for (idx, hash) in hashes.iter_mut().enumerate() {
                    let sel = unsafe { selection::get_unchecked(selection, idx) };

                    if validity.value(sel) {
                        let val = unsafe { values.get_unchecked(sel) };
                        *hash = combine_hashes(val.hash_one(), *hash);
                    } else {
                        *hash = combine_hashes(null_hash_value(), *hash);
                    }
                }
            }
            None => {
                let values = S::get_storage(&array.data)?;

                for (idx, hash) in hashes.iter_mut().enumerate() {
                    let sel = unsafe { selection::get_unchecked(selection, idx) };
                    let val = unsafe { values.get_unchecked(sel) };
                    *hash = combine_hashes(val.hash_one(), *hash);
                }
            }
        }

        Ok(())
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
