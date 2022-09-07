//! Hash utilities for columns.
use super::Column;
use crate::arrow::datatype::DataType;
use crate::errors::{internal, Result};
use arrow2::array::{Array, BinaryArray, PrimitiveArray, Utf8Array};
use arrow2::types::NativeType;
use fasthash::{xx, FastHasher};
use std::hash::{Hash, Hasher};

/// Hash all rows in a column.
pub fn hash_column(col: &Column) -> Result<Vec<u64>> {
    let hashes = vec![0; col.len()];

    match col.get_datatype()? {
        DataType::Null => (),
        other => return Err(internal!("cannot hash data type: {:?}", other)),
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
