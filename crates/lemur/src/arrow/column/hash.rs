//! Hash utilities for columns.
use super::Column;
use crate::arrow::datatype::DataType;
use crate::errors::{internal, Result};
use arrow2::array::{
    Array, BinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    PrimitiveArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array, Utf8Array,
};
use arrow2::types::NativeType;
use fasthash::{xx, FastHasher};
use std::hash::{Hash, Hasher};

macro_rules! downcast_and_hash {
    ($type:ty, $arr:expr, $hash_fn:ident, $buf:ident, $combine:ident) => {
        let arr = $arr.as_any().downcast_ref::<$type>().unwrap();
        $hash_fn(arr, &mut $buf, $combine)
    };
}

/// Hash all rows in a column.
pub fn hash_column(cols: &[Column]) -> Result<Vec<u64>> {
    let mut hashes = vec![0; cols.get(0).and_then(|col| Some(col.len())).unwrap_or(0)];
    let combine = cols.len() > 1;

    for col in cols {
        match col.get_datatype()? {
            DataType::Null => (),
            DataType::Int8 => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_primitive_array::<i8>(arr, &mut hashes, combine);
            }
            DataType::Int16 => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_primitive_array::<i16>(arr, &mut hashes, combine);
            }
            DataType::Int32 => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_primitive_array::<i32>(arr, &mut hashes, combine);
            }
            DataType::Int64 => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_primitive_array::<i64>(arr, &mut hashes, combine);
            }
            DataType::Uint8 => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_primitive_array::<u8>(arr, &mut hashes, combine);
            }
            DataType::Uint16 => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_primitive_array::<u16>(arr, &mut hashes, combine);
            }
            DataType::Uint32 => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_primitive_array::<u32>(arr, &mut hashes, combine);
            }
            DataType::Uint64 => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_primitive_array::<u64>(arr, &mut hashes, combine);
            }
            DataType::Float32 => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_float32_array(arr, &mut hashes, combine);
            }
            DataType::Float64 => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_float32_array(arr, &mut hashes, combine);
            }
            DataType::Utf8 => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_utf8_array(arr, &mut hashes, combine);
            }
            DataType::Binary => {
                let arr = col.0.as_any().downcast_ref().unwrap();
                hash_binary_array(arr, &mut hashes, combine);
            }
            other => return Err(internal!("cannot hash data type: {:?}", other)),
        }
    }

    Ok(hashes)
}

/// Combine two hash values.
///
/// Taken from boost:
/// rhs + 0x9e3779b9 + (lhs << 6) + (lhs >> 2);
fn combine_hashes(a: u64, b: u64) -> u64 {
    b.wrapping_add(0x9e3779b9)
        .wrapping_add(a << 6)
        .wrapping_add(b >> 2)
}

/// Hash a primitive array, putting the hashes into `buf`.
///
/// Null values leave the existing hash in `buf` unchanged.
///
/// If `combine` is true, the output hash for a value will be combined with the
/// hash in `buf`.
fn hash_primitive_array<T: NativeType + Hash>(
    arr: &PrimitiveArray<T>,
    buf: &mut [u64],
    combine: bool,
) {
    if arr.null_count() == 0 {
        if combine {
            for (value, hash) in arr.values_iter().zip(buf.iter_mut()) {
                *hash = hash_one(value);
            }
        } else {
            for (value, hash) in arr.values_iter().zip(buf.iter_mut()) {
                *hash = combine_hashes(*hash, hash_one(value));
            }
        }
    } else {
        if combine {
            for (idx, (value, hash)) in arr.values_iter().zip(buf.iter_mut()).enumerate() {
                if arr.is_valid(idx) {
                    *hash = hash_one(value);
                }
            }
        } else {
            for (idx, (value, hash)) in arr.values_iter().zip(buf.iter_mut()).enumerate() {
                if arr.is_valid(idx) {
                    *hash = hash_one(value);
                }
            }
        }
    }
}

macro_rules! hash_float_array_body {
    ($arr:ident, $buf:ident, $combine:ident) => {{
        if $arr.null_count() == 0 {
            if $combine {
                for (value, hash) in $arr.values_iter().zip($buf.iter_mut()) {
                    *hash = hash_one(value.to_le_bytes());
                }
            } else {
                for (value, hash) in $arr.values_iter().zip($buf.iter_mut()) {
                    *hash = combine_hashes(*hash, hash_one(value.to_le_bytes()));
                }
            }
        } else {
            if $combine {
                for (idx, (value, hash)) in $arr.values_iter().zip($buf.iter_mut()).enumerate() {
                    if $arr.is_valid(idx) {
                        *hash = hash_one(value.to_le_bytes());
                    }
                }
            } else {
                for (idx, (value, hash)) in $arr.values_iter().zip($buf.iter_mut()).enumerate() {
                    if $arr.is_valid(idx) {
                        *hash = hash_one(value.to_le_bytes());
                    }
                }
            }
        }
    }};
}

/// Hash a float32 array.
fn hash_float32_array(arr: &PrimitiveArray<f32>, buf: &mut [u64], combine: bool) {
    hash_float_array_body!(arr, buf, combine)
}

/// Hash a float64 array.
fn hash_float64_array(arr: &PrimitiveArray<f64>, buf: &mut [u64], combine: bool) {
    hash_float_array_body!(arr, buf, combine)
}

macro_rules! hash_binary_or_utf8_array_body {
    ($arr:ident, $buf:ident, $combine:ident) => {{
        if $arr.null_count() == 0 {
            if $combine {
                for (idx, hash) in $buf.iter_mut().enumerate() {
                    *hash = hash_one($arr.value(idx));
                }
            } else {
                for (idx, hash) in $buf.iter_mut().enumerate() {
                    *hash = combine_hashes(*hash, hash_one($arr.value(idx)));
                }
            }
        } else {
            if $combine {
                for (idx, hash) in $buf.iter_mut().enumerate() {
                    if $arr.is_valid(idx) {
                        *hash = hash_one($arr.value(idx));
                    }
                }
            } else {
                for (idx, hash) in $buf.iter_mut().enumerate() {
                    if $arr.is_valid(idx) {
                        *hash = hash_one($arr.value(idx));
                    }
                }
            }
        }
    }};
}

/// Hash a utf8 array.
fn hash_utf8_array(arr: &Utf8Array<i32>, buf: &mut [u64], combine: bool) {
    hash_binary_or_utf8_array_body!(arr, buf, combine)
}

/// Hash a binary array.
fn hash_binary_array(arr: &BinaryArray<i32>, buf: &mut [u64], combine: bool) {
    hash_binary_or_utf8_array_body!(arr, buf, combine)
}

fn hash_one<T: Hash>(val: T) -> u64 {
    let mut hasher = xx::Hasher64::new();
    val.hash(&mut hasher);
    hasher.finish()
}
