use ahash::RandomState;
use rayexec_bullet::{
    array::{Array, BooleanArray, OffsetIndex, PrimitiveArray, VarlenArray, VarlenType},
    row::ScalarRow,
    scalar::{interval::Interval, ScalarValue},
};
use rayexec_error::{RayexecError, Result};

/// State used for all hashing operations during physical execution.
pub const HASH_RANDOM_STATE: RandomState = RandomState::with_seeds(0, 0, 0, 0);

/// Get the partition to use for a hash.
///
/// This should be used for hash repartitions, hash joins, hash aggregates, and
/// whatever else requires consistent hash to partition mappings.
pub const fn partition_for_hash(hash: u64, partitions: usize) -> usize {
    hash as usize % partitions
}

/// Hash every row in the provided arrays, writing the values to `hashes`.
///
/// All arrays provided must be of the same length, and the provided hash buffer
/// must equal that length.
pub fn hash_arrays<'a>(arrays: &[&Array], hashes: &'a mut [u64]) -> Result<&'a mut [u64]> {
    for (idx, array) in arrays.iter().enumerate() {
        let combine_hash = idx > 0;

        match array {
            Array::Null(_) => hash_null(hashes, combine_hash),
            Array::Boolean(arr) => hash_bool(arr, hashes, combine_hash),
            Array::Float32(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::Float64(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::Int8(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::Int16(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::Int32(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::Int64(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::Int128(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::UInt8(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::UInt16(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::UInt32(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::UInt64(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::UInt128(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::Decimal64(arr) => hash_primitive(arr.get_primitive(), hashes, combine_hash),
            Array::Decimal128(arr) => hash_primitive(arr.get_primitive(), hashes, combine_hash),
            Array::Date32(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::Date64(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::TimestampSeconds(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::TimestampMilliseconds(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::TimestampMicroseconds(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::TimestampNanoseconds(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::Interval(arr) => hash_primitive(arr, hashes, combine_hash),
            Array::Utf8(arr) => hash_varlen(arr, hashes, combine_hash),
            Array::LargeUtf8(arr) => hash_varlen(arr, hashes, combine_hash),
            Array::Binary(arr) => hash_varlen(arr, hashes, combine_hash),
            Array::LargeBinary(arr) => hash_varlen(arr, hashes, combine_hash),
            Array::Struct(_) => {
                // Yet
                return Err(RayexecError::new("hashing struct arrays not supported"));
            }
        }
    }

    Ok(hashes)
}

/// Hash a row.
#[allow(dead_code)]
pub fn hash_row(row: &ScalarRow) -> Result<u64> {
    let mut result = 0;
    for (idx, scalar) in row.iter().enumerate() {
        let combine_hash = idx > 0;

        let scalar_hash = match scalar {
            ScalarValue::Null => null_hash_value(),
            ScalarValue::Boolean(v) => v.hash_one(),
            ScalarValue::Float32(v) => v.hash_one(),
            ScalarValue::Float64(v) => v.hash_one(),
            ScalarValue::Int8(v) => v.hash_one(),
            ScalarValue::Int16(v) => v.hash_one(),
            ScalarValue::Int32(v) => v.hash_one(),
            ScalarValue::Int64(v) => v.hash_one(),
            ScalarValue::Int128(v) => v.hash_one(),
            ScalarValue::UInt8(v) => v.hash_one(),
            ScalarValue::UInt16(v) => v.hash_one(),
            ScalarValue::UInt32(v) => v.hash_one(),
            ScalarValue::UInt64(v) => v.hash_one(),
            ScalarValue::UInt128(v) => v.hash_one(),
            ScalarValue::Decimal64(v) => v.value.hash_one(),
            ScalarValue::Decimal128(v) => v.value.hash_one(),
            ScalarValue::Date32(v) => v.hash_one(),
            ScalarValue::Date64(v) => v.hash_one(),
            ScalarValue::TimestampSeconds(v) => v.hash_one(),
            ScalarValue::TimestampMilliseconds(v) => v.hash_one(),
            ScalarValue::TimestampMicroseconds(v) => v.hash_one(),
            ScalarValue::TimestampNanoseconds(v) => v.hash_one(),
            ScalarValue::Interval(v) => v.hash_one(),
            ScalarValue::Utf8(v) => v.hash_one(),
            ScalarValue::LargeUtf8(v) => v.hash_one(),
            ScalarValue::Binary(v) => v.hash_one(),
            ScalarValue::LargeBinary(v) => v.hash_one(),
            ScalarValue::Struct(_) => {
                // Yet
                return Err(RayexecError::new("hashing struct values not supported"));
            }
        };

        if combine_hash {
            result = combine_hashes(scalar_hash, result);
        } else {
            result = scalar_hash;
        }
    }

    Ok(result)
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

/// Combines two hashes into one hash
///
/// This implementation came from datafusion.
const fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

/// All nulls should hash to the same value.
///
/// _What_ that value is is arbitrary, but it needs to be consistent.
fn null_hash_value() -> u64 {
    HASH_RANDOM_STATE.hash_one(1)
}

fn hash_null(hashes: &mut [u64], combine: bool) {
    let null_hash = null_hash_value();

    if combine {
        for hash in hashes.iter_mut() {
            *hash = combine_hashes(null_hash, *hash);
        }
    } else {
        for hash in hashes.iter_mut() {
            *hash = null_hash;
        }
    }
}

fn hash_bool(array: &BooleanArray, hashes: &mut [u64], combine: bool) {
    assert_eq!(
        array.len(),
        hashes.len(),
        "Hashes buffer should be same length as array"
    );

    let values = array.values();
    match array.validity() {
        Some(_bitmap) => {
            // TODO: Nulls
            unimplemented!()
        }
        None => {
            if combine {
                for (val, hash) in values.iter().zip(hashes.iter_mut()) {
                    *hash = combine_hashes(val.hash_one(), *hash);
                }
            } else {
                for (val, hash) in values.iter().zip(hashes.iter_mut()) {
                    *hash = val.hash_one();
                }
            }
        }
    }
}

/// Hash a primitive array.
fn hash_primitive<T: HashValue>(array: &PrimitiveArray<T>, hashes: &mut [u64], combine: bool) {
    assert_eq!(
        array.len(),
        hashes.len(),
        "Hashes buffer should be same length as array"
    );

    let values = array.values();
    match array.validity() {
        Some(_bitmap) => {
            // TODO: Nulls
            unimplemented!()
        }
        None => {
            if combine {
                for (val, hash) in values.as_ref().iter().zip(hashes.iter_mut()) {
                    *hash = combine_hashes(val.hash_one(), *hash);
                }
            } else {
                for (val, hash) in values.as_ref().iter().zip(hashes.iter_mut()) {
                    *hash = val.hash_one();
                }
            }
        }
    }
}

/// Hash a varlen array.
fn hash_varlen<T, O>(array: &VarlenArray<T, O>, hashes: &mut [u64], combine: bool)
where
    T: VarlenType + HashValue + ?Sized,
    O: OffsetIndex,
{
    assert_eq!(
        array.len(),
        hashes.len(),
        "Hashes buffer should be same length as array"
    );

    let values_iter = array.values_iter();
    match array.validity() {
        Some(_bitmap) => {
            // TODO: Nulls
            unimplemented!()
        }
        None => {
            if combine {
                for (val, hash) in values_iter.zip(hashes.iter_mut()) {
                    *hash = combine_hashes(val.hash_one(), *hash);
                }
            } else {
                for (val, hash) in values_iter.zip(hashes.iter_mut()) {
                    *hash = val.hash_one();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::{Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn array_hash_row_hash_equivalent() {
        // Hashing a slice of a arrays should produce the same hashes as the row
        // representation of those same values.

        let arrays = [
            &Array::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
            &Array::Int32(Int32Array::from_iter([1, 2, 3])),
        ];
        let mut hashes = vec![0; 3];

        // Hash the arrays.
        hash_arrays(&arrays, &mut hashes).unwrap();

        // Sanity check just to make sure we're hashing.
        assert_ne!(vec![0; 3], hashes);

        // Now hash the row representations.
        let mut row_hashes = vec![0; 3];
        for idx in 0..3 {
            let row = ScalarRow::try_new_from_arrays(&arrays, idx).unwrap();
            row_hashes[idx] = hash_row(&row).unwrap();
        }

        assert_eq!(hashes, row_hashes);
    }
}
