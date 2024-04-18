use ahash::RandomState;
use arrow::datatypes::{ArrowNativeType, DataType};
use arrow::{downcast_dictionary_array, downcast_primitive_array};
use arrow_array::cast::{
    as_boolean_array, as_fixed_size_list_array, as_generic_binary_array, as_large_list_array,
    as_largestring_array, as_list_array, as_primitive_array, as_string_array, as_struct_array,
};
use arrow_array::types::{
    ArrowDictionaryKeyType, ArrowPrimitiveType, Decimal128Type, Decimal256Type,
};
use arrow_array::{
    Array, ArrayAccessor, ArrayRef, DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray,
    GenericListArray, OffsetSizeTrait, PrimitiveArray, StructArray, UInt64Array,
};
use arrow_buffer::i256;
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;

use crate::types::batch::DataBatch;

use super::take::take_indexes;

/// State used for all hashing operations during physical execution.
pub const HASH_RANDOM_STATE: RandomState = RandomState::with_seeds(0, 0, 0, 0);

/// Get the partition to use for a hash.
///
/// This should be used for hash repartitions, hash joins, hash aggregates, and
/// whatever else requires consistent hash to partition mappings.
pub const fn partition_for_hash(hash: u64, partitions: usize) -> usize {
    hash as usize % partitions
}

#[derive(Debug)]
pub struct HashPartitionedBatch {
    pub batch: DataBatch,
    pub hashes: Vec<u64>,
}

/// Partition a batch based on an arbitrary list of hashes.
pub fn hash_partition_batch(
    batch: &DataBatch,
    hashes: &[u64],
    partitions: usize,
) -> Result<Vec<HashPartitionedBatch>> {
    let mut outputs = Vec::with_capacity(partitions);

    let mut row_indexes: Vec<Vec<usize>> = (0..partitions)
        .map(|_| Vec::with_capacity(batch.num_rows() / partitions))
        .collect();

    for (row_idx, hash) in hashes.iter().enumerate() {
        row_indexes[partition_for_hash(*hash, partitions)].push(row_idx);
    }

    for (partition_idx, partition_rows) in row_indexes.into_iter().enumerate() {
        // Get the hashes corresponding to the rows for this output
        // partition.
        let batch_hashes = take_indexes(&hashes, &partition_rows);

        let partition_rows =
            UInt64Array::from_iter(partition_rows.into_iter().map(|idx| idx as u64));

        // Get the rows for this batch.
        let cols = batch
            .columns()
            .iter()
            .map(|col| arrow::compute::take(col.as_ref(), &partition_rows, None))
            .collect::<Result<Vec<_>, _>>()?;

        let batch = DataBatch::try_new(cols)?;
        outputs.push(HashPartitionedBatch {
            batch,
            hashes: batch_hashes,
        })
    }

    Ok(outputs)
}

/// Hash every row in the provided arrays, writing the values to `hashes`.
///
/// This and the below functions were adapted from datafusion.
pub fn build_hashes<'a>(arrays: &[&ArrayRef], hashes: &'a mut [u64]) -> Result<&'a mut [u64]> {
    for (i, col) in arrays.iter().enumerate() {
        let array = col.as_ref();
        // combine hashes with `combine_hashes` for all columns besides the first
        let rehash = i >= 1;
        downcast_primitive_array! {
            array => hash_array_primitive(array, hashes, rehash),
            DataType::Null => hash_null(hashes, rehash),
            DataType::Boolean => hash_array(as_boolean_array(array), hashes, rehash),
            DataType::Utf8 => hash_array(as_string_array(array), hashes, rehash),
            DataType::LargeUtf8 => hash_array(as_largestring_array(array), hashes, rehash),
            DataType::Binary => hash_array(as_generic_binary_array::<i32>(array), hashes, rehash),
            DataType::LargeBinary => hash_array(as_generic_binary_array::<i64>(array), hashes, rehash),
            DataType::FixedSizeBinary(_) => {
                let array: &FixedSizeBinaryArray = array.as_any().downcast_ref().unwrap();
                hash_array(array, hashes, rehash)
            }
            DataType::Decimal128(_, _) => {
                let array = as_primitive_array::<Decimal128Type>(array);
                hash_array_primitive(array,  hashes, rehash)
            }
            DataType::Decimal256(_, _) => {
                let array = as_primitive_array::<Decimal256Type>(array);
                hash_array_primitive(array,  hashes, rehash)
            }
            DataType::Dictionary(_, _) => downcast_dictionary_array! {
                array => hash_dictionary(array,  hashes, rehash)?,
                _ => unreachable!()
            }
            DataType::Struct(_) => {
                let array = as_struct_array(array);
                hash_struct_array(array,  hashes)?;
            }
            DataType::List(_) => {
                let array = as_list_array(array);
                hash_list_array(array,  hashes)?;
            }
            DataType::LargeList(_) => {
                let array = as_large_list_array(array);
                hash_list_array(array, hashes)?;
            }
            DataType::FixedSizeList(_,_) => {
                let array = as_fixed_size_list_array(array);
                hash_fixed_list_array(array, hashes)?;
            }
            _ => {
                return Err(RayexecError::new(format!("Unsupported type for hashing: {}", col.data_type())))
                    }
        }
    }
    Ok(hashes)
}

// Combines two hashes into one hash
#[inline]
fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

fn hash_null(hashes_buffer: &'_ mut [u64], mul_col: bool) {
    if mul_col {
        hashes_buffer.iter_mut().for_each(|hash| {
            // stable hash for null value
            *hash = combine_hashes(HASH_RANDOM_STATE.hash_one(1), *hash);
        })
    } else {
        hashes_buffer.iter_mut().for_each(|hash| {
            *hash = HASH_RANDOM_STATE.hash_one(1);
        })
    }
}

pub trait HashValue {
    fn hash_one(&self) -> u64;
}

impl<'a, T: HashValue + ?Sized> HashValue for &'a T {
    fn hash_one(&self) -> u64 {
        T::hash_one(self)
    }
}

macro_rules! hash_value {
    ($($t:ty),+) => {
        $(impl HashValue for $t {
            fn hash_one(&self) -> u64 {
                HASH_RANDOM_STATE.hash_one(self)
            }
        })+
    };
}
hash_value!(i8, i16, i32, i64, i128, i256, u8, u16, u32, u64);
hash_value!(bool, str, [u8]);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(impl HashValue for $t {
            fn hash_one(&self) -> u64 {
                HASH_RANDOM_STATE.hash_one(<$i>::from_ne_bytes(self.to_ne_bytes()))
            }
        })+
    };
}
hash_float_value!((half::f16, u16), (f32, u32), (f64, u64));

/// Builds hash values of PrimitiveArray and writes them into `hashes_buffer`
/// If `rehash==true` this combines the previous hash value in the buffer
/// with the new hash using `combine_hashes`
fn hash_array_primitive<T>(array: &PrimitiveArray<T>, hashes_buffer: &mut [u64], rehash: bool)
where
    T: ArrowPrimitiveType,
    <T as arrow_array::ArrowPrimitiveType>::Native: HashValue,
{
    assert_eq!(
        hashes_buffer.len(),
        array.len(),
        "hashes_buffer and array should be of equal length"
    );

    if array.null_count() == 0 {
        if rehash {
            for (hash, &value) in hashes_buffer.iter_mut().zip(array.values().iter()) {
                *hash = combine_hashes(value.hash_one(), *hash);
            }
        } else {
            for (hash, &value) in hashes_buffer.iter_mut().zip(array.values().iter()) {
                *hash = value.hash_one();
            }
        }
    } else if rehash {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                let value = unsafe { array.value_unchecked(i) };
                *hash = combine_hashes(value.hash_one(), *hash);
            }
        }
    } else {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                let value = unsafe { array.value_unchecked(i) };
                *hash = value.hash_one();
            }
        }
    }
}

/// Hashes one array into the `hashes_buffer`
/// If `rehash==true` this combines the previous hash value in the buffer
/// with the new hash using `combine_hashes`
fn hash_array<T>(array: T, hashes_buffer: &mut [u64], rehash: bool)
where
    T: ArrayAccessor,
    T::Item: HashValue,
{
    assert_eq!(
        hashes_buffer.len(),
        array.len(),
        "hashes_buffer and array should be of equal length"
    );

    if array.null_count() == 0 {
        if rehash {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                let value = unsafe { array.value_unchecked(i) };
                *hash = combine_hashes(value.hash_one(), *hash);
            }
        } else {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                let value = unsafe { array.value_unchecked(i) };
                *hash = value.hash_one();
            }
        }
    } else if rehash {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                let value = unsafe { array.value_unchecked(i) };
                *hash = combine_hashes(value.hash_one(), *hash);
            }
        }
    } else {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                let value = unsafe { array.value_unchecked(i) };
                *hash = value.hash_one();
            }
        }
    }
}

/// Hash the values in a dictionary array
fn hash_dictionary<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    hashes_buffer: &mut [u64],
    multi_col: bool,
) -> Result<()> {
    // Hash each dictionary value once, and then use that computed
    // hash for each key value to avoid a potentially expensive
    // redundant hashing for large dictionary elements (e.g. strings)
    let values = array.values();
    let mut dict_hashes = vec![0; values.len()];
    build_hashes(&[values], &mut dict_hashes)?;

    // combine hash for each index in values
    if multi_col {
        for (hash, key) in hashes_buffer.iter_mut().zip(array.keys().iter()) {
            if let Some(key) = key {
                *hash = combine_hashes(dict_hashes[key.as_usize()], *hash)
            } // no update for Null, consistent with other hashes
        }
    } else {
        for (hash, key) in hashes_buffer.iter_mut().zip(array.keys().iter()) {
            if let Some(key) = key {
                *hash = dict_hashes[key.as_usize()]
            } // no update for Null, consistent with other hashes
        }
    }
    Ok(())
}

fn hash_struct_array(array: &StructArray, hashes_buffer: &mut [u64]) -> Result<()> {
    let nulls = array.nulls();
    let row_len = array.len();

    let valid_row_indices: Vec<usize> = if let Some(nulls) = nulls {
        nulls.valid_indices().collect()
    } else {
        (0..row_len).collect()
    };

    // Create hashes for each row that combines the hashes over all the column at that row.
    let mut values_hashes = vec![0u64; row_len];
    let cols: Vec<_> = array.columns().iter().collect();
    build_hashes(&cols, &mut values_hashes)?;

    for i in valid_row_indices {
        let hash = &mut hashes_buffer[i];
        *hash = combine_hashes(*hash, values_hashes[i]);
    }

    Ok(())
}

fn hash_list_array<OffsetSize>(
    array: &GenericListArray<OffsetSize>,
    hashes_buffer: &mut [u64],
) -> Result<()>
where
    OffsetSize: OffsetSizeTrait,
{
    let values = array.values();
    let offsets = array.value_offsets();
    let nulls = array.nulls();
    let mut values_hashes = vec![0u64; values.len()];
    build_hashes(&[values], &mut values_hashes)?;
    if let Some(nulls) = nulls {
        for (i, (start, stop)) in offsets.iter().zip(offsets.iter().skip(1)).enumerate() {
            if nulls.is_valid(i) {
                let hash = &mut hashes_buffer[i];
                for values_hash in &values_hashes[start.as_usize()..stop.as_usize()] {
                    *hash = combine_hashes(*hash, *values_hash);
                }
            }
        }
    } else {
        for (i, (start, stop)) in offsets.iter().zip(offsets.iter().skip(1)).enumerate() {
            let hash = &mut hashes_buffer[i];
            for values_hash in &values_hashes[start.as_usize()..stop.as_usize()] {
                *hash = combine_hashes(*hash, *values_hash);
            }
        }
    }
    Ok(())
}

fn hash_fixed_list_array(array: &FixedSizeListArray, hashes_buffer: &mut [u64]) -> Result<()> {
    let values = array.values();
    let value_len = array.value_length();
    let offset_size = value_len as usize / array.len();
    let nulls = array.nulls();
    let mut values_hashes = vec![0u64; values.len()];
    build_hashes(&[values], &mut values_hashes)?;
    if let Some(nulls) = nulls {
        for i in 0..array.len() {
            if nulls.is_valid(i) {
                let hash = &mut hashes_buffer[i];
                for values_hash in &values_hashes[i * offset_size..(i + 1) * offset_size] {
                    *hash = combine_hashes(*hash, *values_hash);
                }
            }
        }
    } else {
        for i in 0..array.len() {
            let hash = &mut hashes_buffer[i];
            for values_hash in &values_hashes[i * offset_size..(i + 1) * offset_size] {
                *hash = combine_hashes(*hash, *values_hash);
            }
        }
    }
    Ok(())
}
