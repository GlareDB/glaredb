use std::fmt::Debug;

use half::f16;
use rayexec_error::{RayexecError, Result, ResultExt};

use super::builder::{ArrayDataBuffer, BooleanBuffer, GermanVarlenBuffer, PrimitiveBuffer};
use crate::array::{ArrayData, BinaryData};
use crate::scalar::interval::Interval;
use crate::storage::{
    AddressableStorage,
    BooleanStorageRef,
    ContiguousVarlenStorageSlice,
    GermanVarlenStorageSlice,
    PrimitiveStorageSlice,
    UntypedNullStorage,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalType {
    UntypedNull,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Float16,
    Float32,
    Float64,
    Interval,
    Binary,
    Utf8,
}

impl PhysicalType {
    pub fn zeroed_array_data(&self, len: usize) -> ArrayData {
        match self {
            Self::UntypedNull => UntypedNullStorage(len).into(),
            Self::Boolean => BooleanBuffer::with_len(len).into_data(),
            Self::Int8 => PrimitiveBuffer::<i8>::with_len(len).into_data(),
            Self::Int16 => PrimitiveBuffer::<i16>::with_len(len).into_data(),
            Self::Int32 => PrimitiveBuffer::<i32>::with_len(len).into_data(),
            Self::Int64 => PrimitiveBuffer::<i64>::with_len(len).into_data(),
            Self::Int128 => PrimitiveBuffer::<i128>::with_len(len).into_data(),
            Self::UInt8 => PrimitiveBuffer::<u8>::with_len(len).into_data(),
            Self::UInt16 => PrimitiveBuffer::<u16>::with_len(len).into_data(),
            Self::UInt32 => PrimitiveBuffer::<u32>::with_len(len).into_data(),
            Self::UInt64 => PrimitiveBuffer::<u64>::with_len(len).into_data(),
            Self::UInt128 => PrimitiveBuffer::<u128>::with_len(len).into_data(),
            Self::Float16 => PrimitiveBuffer::<f16>::with_len(len).into_data(),
            Self::Float32 => PrimitiveBuffer::<f32>::with_len(len).into_data(),
            Self::Float64 => PrimitiveBuffer::<f64>::with_len(len).into_data(),
            Self::Interval => PrimitiveBuffer::<Interval>::with_len(len).into_data(),
            Self::Binary => GermanVarlenBuffer::<[u8]>::with_len(len).into_data(),
            Self::Utf8 => GermanVarlenBuffer::<str>::with_len(len).into_data(),
        }
    }
}

/// Types able to convert themselves to byte slices.
pub trait AsBytes {
    fn as_bytes(&self) -> &[u8];
}

impl AsBytes for str {
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsBytes for &str {
    fn as_bytes(&self) -> &[u8] {
        (*self).as_bytes()
    }
}

impl AsBytes for [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl AsBytes for &[u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

/// Types that can be converted from bytes.
///
/// This should not be implemented for `&str`/`&[u8]`.
pub trait VarlenType: Debug + PartialEq + Eq {
    fn try_from_bytes(bytes: &[u8]) -> Result<&Self>;
}

impl VarlenType for str {
    fn try_from_bytes(bytes: &[u8]) -> Result<&Self> {
        std::str::from_utf8(bytes).context("Bytes not valid utf8")
    }
}

impl VarlenType for [u8] {
    fn try_from_bytes(bytes: &[u8]) -> Result<&Self> {
        Ok(bytes)
    }
}

/// Helper trait for getting the underlying data for an array.
///
/// Contains a lifetime to enable tying the returned storage to the provided
/// array data.
pub trait PhysicalStorage<'a> {
    type Storage: AddressableStorage;

    /// Gets the storage for the array that we can access directly.
    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage>;
}

/// Type that's able to be used for any physical type.
///
/// While this allows any array type to used in the executors, there's no way to
/// actually get the underlying values. This is useful if the values aren't
/// actually needed (e.g. COUNT(*)).
pub struct PhysicalAny;

impl<'a> PhysicalStorage<'a> for PhysicalAny {
    type Storage = UnitStorage;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        Ok(UnitStorage(data.len()))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UnitStorage(usize);

impl AddressableStorage for UnitStorage {
    type T = ();

    fn len(&self) -> usize {
        self.0
    }

    fn get(&self, idx: usize) -> Option<Self::T> {
        if idx >= self.0 {
            return None;
        }
        Some(())
    }

    unsafe fn get_unchecked(&self, idx: usize) -> Self::T {
        self.get(idx).unwrap()
    }
}

pub struct PhysicalUntypedNull;

impl<'a> PhysicalStorage<'a> for PhysicalUntypedNull {
    type Storage = UntypedNullStorage;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::UntypedNull(s) => Ok(*s),
            _ => Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalBool;

impl<'a> PhysicalStorage<'a> for PhysicalBool {
    type Storage = BooleanStorageRef<'a>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Boolean(storage) => Ok(storage.as_boolean_storage_ref()),
            _ => Err(RayexecError::new("invalid storage, expected boolean")),
        }
    }
}

pub struct PhysicalI8;

impl<'a> PhysicalStorage<'a> for PhysicalI8 {
    type Storage = PrimitiveStorageSlice<'a, i8>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Int8(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected int8")),
        }
    }
}

pub struct PhysicalI16;

impl<'a> PhysicalStorage<'a> for PhysicalI16 {
    type Storage = PrimitiveStorageSlice<'a, i16>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Int16(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected int16")),
        }
    }
}

pub struct PhysicalI32;

impl<'a> PhysicalStorage<'a> for PhysicalI32 {
    type Storage = PrimitiveStorageSlice<'a, i32>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Int32(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected int32")),
        }
    }
}

pub struct PhysicalI64;

impl<'a> PhysicalStorage<'a> for PhysicalI64 {
    type Storage = PrimitiveStorageSlice<'a, i64>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Int64(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected int64")),
        }
    }
}

pub struct PhysicalI128;

impl<'a> PhysicalStorage<'a> for PhysicalI128 {
    type Storage = PrimitiveStorageSlice<'a, i128>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Int128(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected int128")),
        }
    }
}

pub struct PhysicalU8;

impl<'a> PhysicalStorage<'a> for PhysicalU8 {
    type Storage = PrimitiveStorageSlice<'a, u8>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::UInt8(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected u8")),
        }
    }
}

pub struct PhysicalU16;

impl<'a> PhysicalStorage<'a> for PhysicalU16 {
    type Storage = PrimitiveStorageSlice<'a, u16>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::UInt16(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected u16")),
        }
    }
}

pub struct PhysicalU32;

impl<'a> PhysicalStorage<'a> for PhysicalU32 {
    type Storage = PrimitiveStorageSlice<'a, u32>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::UInt32(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected u32")),
        }
    }
}

pub struct PhysicalU64;

impl<'a> PhysicalStorage<'a> for PhysicalU64 {
    type Storage = PrimitiveStorageSlice<'a, u64>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::UInt64(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected u64")),
        }
    }
}

pub struct PhysicalU128;

impl<'a> PhysicalStorage<'a> for PhysicalU128 {
    type Storage = PrimitiveStorageSlice<'a, u128>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::UInt128(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected u128")),
        }
    }
}

pub struct PhysicalF16;

impl<'a> PhysicalStorage<'a> for PhysicalF16 {
    type Storage = PrimitiveStorageSlice<'a, f16>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Float16(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected f32")),
        }
    }
}

pub struct PhysicalF32;

impl<'a> PhysicalStorage<'a> for PhysicalF32 {
    type Storage = PrimitiveStorageSlice<'a, f32>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Float32(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected f32")),
        }
    }
}

pub struct PhysicalF64;

impl<'a> PhysicalStorage<'a> for PhysicalF64 {
    type Storage = PrimitiveStorageSlice<'a, f64>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Float64(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected f64")),
        }
    }
}

pub struct PhysicalInterval;

impl<'a> PhysicalStorage<'a> for PhysicalInterval {
    type Storage = PrimitiveStorageSlice<'a, Interval>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Interval(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected interval")),
        }
    }
}

pub struct PhysicalBinary;

impl<'a> PhysicalStorage<'a> for PhysicalBinary {
    type Storage = BinaryDataStorage<'a>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Binary(binary) => match binary {
                BinaryData::Binary(b) => {
                    Ok(BinaryDataStorage::Binary(b.as_contiguous_storage_slice()))
                }
                BinaryData::LargeBinary(b) => Ok(BinaryDataStorage::LargeBinary(
                    b.as_contiguous_storage_slice(),
                )),
                BinaryData::German(b) => Ok(BinaryDataStorage::German(b.as_german_storage_slice())),
            },
            _ => Err(RayexecError::new("invalid storage, expected binary")),
        }
    }
}

pub struct PhysicalUtf8;

impl<'a> PhysicalStorage<'a> for PhysicalUtf8 {
    type Storage = StrDataStorage<'a>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Binary(binary) => match binary {
                BinaryData::Binary(b) => {
                    Ok(BinaryDataStorage::Binary(b.as_contiguous_storage_slice()).into())
                }
                BinaryData::LargeBinary(b) => {
                    Ok(BinaryDataStorage::LargeBinary(b.as_contiguous_storage_slice()).into())
                }
                BinaryData::German(b) => {
                    Ok(BinaryDataStorage::German(b.as_german_storage_slice()).into())
                }
            },
            _ => Err(RayexecError::new("invalid storage")),
        }
    }
}

#[derive(Debug)]
pub enum BinaryDataStorage<'a> {
    Binary(ContiguousVarlenStorageSlice<'a, i32>),
    LargeBinary(ContiguousVarlenStorageSlice<'a, i64>),
    German(GermanVarlenStorageSlice<'a>),
}

impl<'a> AddressableStorage for BinaryDataStorage<'a> {
    type T = &'a [u8];

    fn len(&self) -> usize {
        match self {
            Self::Binary(s) => s.len(),
            Self::LargeBinary(s) => s.len(),
            Self::German(s) => s.len(),
        }
    }

    fn get(&self, idx: usize) -> Option<Self::T> {
        match self {
            Self::Binary(s) => s.get(idx),
            Self::LargeBinary(s) => s.get(idx),
            Self::German(s) => s.get(idx),
        }
    }

    #[inline]
    unsafe fn get_unchecked(&self, idx: usize) -> Self::T {
        match self {
            Self::Binary(s) => s.get_unchecked(idx),
            Self::LargeBinary(s) => s.get_unchecked(idx),
            Self::German(s) => s.get_unchecked(idx),
        }
    }
}

#[derive(Debug)]
pub struct StrDataStorage<'a> {
    inner: BinaryDataStorage<'a>,
}

impl<'a> AddressableStorage for StrDataStorage<'a> {
    type T = &'a str;

    fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    fn get(&self, idx: usize) -> Option<Self::T> {
        let b = self.inner.get(idx)?;
        // SAFETY: Construction of the vector should have already validated the data.
        let s = unsafe { std::str::from_utf8_unchecked(b) };
        Some(s)
    }

    #[inline]
    unsafe fn get_unchecked(&self, idx: usize) -> Self::T {
        let b = self.inner.get_unchecked(idx);
        std::str::from_utf8_unchecked(b) // See above
    }
}

impl<'a> From<BinaryDataStorage<'a>> for StrDataStorage<'a> {
    fn from(value: BinaryDataStorage<'a>) -> Self {
        StrDataStorage { inner: value }
    }
}
