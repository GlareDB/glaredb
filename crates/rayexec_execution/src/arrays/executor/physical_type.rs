use std::fmt::Debug;

use half::f16;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_proto::ProtoConv;

use super::builder::{ArrayDataBuffer, BooleanBuffer, GermanVarlenBuffer, PrimitiveBuffer};
use crate::arrays::array::{Array2, ArrayData2, BinaryData};
use crate::arrays::scalar::interval::Interval;
use crate::arrays::storage::{
    AddressableStorage,
    BooleanStorageRef,
    ContiguousVarlenStorageSlice,
    GermanVarlenStorageSlice,
    ListItemMetadata,
    ListStorage,
    PrimitiveStorageSlice,
    UntypedNull,
    UntypedNullStorage,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalType2 {
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
    List,
}

impl PhysicalType2 {
    pub fn zeroed_array_data(&self, len: usize) -> ArrayData2 {
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
            Self::List => ListStorage {
                metadata: vec![ListItemMetadata::default(); len].into(),
                array: Array2::new_untyped_null_array(0),
            }
            .into(),
        }
    }
}

impl ProtoConv for PhysicalType2 {
    type ProtoType = rayexec_proto::generated::physical_type::PhysicalType;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(match self {
            Self::UntypedNull => Self::ProtoType::UntypedNull,
            Self::Boolean => Self::ProtoType::Boolean,
            Self::Int8 => Self::ProtoType::Int8,
            Self::Int16 => Self::ProtoType::Int16,
            Self::Int32 => Self::ProtoType::Int32,
            Self::Int64 => Self::ProtoType::Int64,
            Self::Int128 => Self::ProtoType::Int128,
            Self::UInt8 => Self::ProtoType::Uint8,
            Self::UInt16 => Self::ProtoType::Uint16,
            Self::UInt32 => Self::ProtoType::Uint32,
            Self::UInt64 => Self::ProtoType::Uint64,
            Self::UInt128 => Self::ProtoType::Uint128,
            Self::Float16 => Self::ProtoType::Float16,
            Self::Float32 => Self::ProtoType::Float32,
            Self::Float64 => Self::ProtoType::Float64,
            Self::Interval => Self::ProtoType::Interval,
            Self::Utf8 => Self::ProtoType::Utf8,
            Self::Binary => Self::ProtoType::Binary,
            Self::List => Self::ProtoType::List,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(match proto {
            Self::ProtoType::InvalidPhysicalType => return Err(RayexecError::new("invalid")),
            Self::ProtoType::UntypedNull => Self::UntypedNull,
            Self::ProtoType::Boolean => Self::Boolean,
            Self::ProtoType::Int8 => Self::Int8,
            Self::ProtoType::Int16 => Self::Int16,
            Self::ProtoType::Int32 => Self::Int32,
            Self::ProtoType::Int64 => Self::Int64,
            Self::ProtoType::Int128 => Self::Int128,
            Self::ProtoType::Uint8 => Self::UInt8,
            Self::ProtoType::Uint16 => Self::UInt16,
            Self::ProtoType::Uint32 => Self::UInt32,
            Self::ProtoType::Uint64 => Self::UInt64,
            Self::ProtoType::Uint128 => Self::UInt128,
            Self::ProtoType::Float16 => Self::Float16,
            Self::ProtoType::Float32 => Self::Float32,
            Self::ProtoType::Float64 => Self::Float64,
            Self::ProtoType::Interval => Self::Interval,
            Self::ProtoType::Utf8 => Self::Utf8,
            Self::ProtoType::Binary => Self::Binary,
            Self::ProtoType::List => Self::List,
        })
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
pub trait PhysicalStorage2: Debug + Sync + Send + Clone + Copy + 'static {
    // /// The type that's stored in the primary buffer.
    // ///
    // /// This should be small and fixed sized.
    // type PrimaryBufferType: Sized + Debug + Default + Sync + Send + Clone + Copy;

    /// The type that gets returned from the underlying array storage.
    type Type<'a>: Sync + Send;
    /// The type of the underlying array storage.
    type Storage<'a>: AddressableStorage<T = Self::Type<'a>>;

    /// Gets the storage for the array that we can access directly.
    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>>;
}

/// Type that's able to be used for any physical type.
///
/// While this allows any array type to used in the executors, there's no way to
/// actually get the underlying values. This is useful if the values aren't
/// actually needed (e.g. COUNT(*)).
#[derive(Debug, Clone, Copy)]
pub struct PhysicalAny;

impl PhysicalStorage2 for PhysicalAny {
    type Type<'a> = ();
    type Storage<'a> = UnitStorage;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
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

#[derive(Debug, Clone, Copy)]
pub struct PhysicalUntypedNull_2;

impl PhysicalStorage2 for PhysicalUntypedNull_2 {
    type Type<'a> = UntypedNull;
    type Storage<'a> = UntypedNullStorage;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::UntypedNull(s) => Ok(*s),
            _ => Err(RayexecError::new("invalid storage")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalBool_2;

impl PhysicalStorage2 for PhysicalBool_2 {
    type Type<'a> = bool;
    type Storage<'a> = BooleanStorageRef<'a>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Boolean(storage) => Ok(storage.as_boolean_storage_ref()),
            _ => Err(RayexecError::new("invalid storage, expected boolean")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalI8_2;

impl PhysicalStorage2 for PhysicalI8_2 {
    type Type<'a> = i8;
    type Storage<'a> = PrimitiveStorageSlice<'a, i8>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Int8(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected int8")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalI16_2;

impl PhysicalStorage2 for PhysicalI16_2 {
    type Type<'a> = i16;
    type Storage<'a> = PrimitiveStorageSlice<'a, i16>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Int16(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected int16")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalI32_2;

impl PhysicalStorage2 for PhysicalI32_2 {
    type Type<'a> = i32;
    type Storage<'a> = PrimitiveStorageSlice<'a, i32>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Int32(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected int32")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalI64_2;

impl PhysicalStorage2 for PhysicalI64_2 {
    type Type<'a> = i64;
    type Storage<'a> = PrimitiveStorageSlice<'a, i64>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Int64(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected int64")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalI128_2;

impl PhysicalStorage2 for PhysicalI128_2 {
    type Type<'a> = i128;
    type Storage<'a> = PrimitiveStorageSlice<'a, i128>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Int128(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected int128")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalU8_2;

impl PhysicalStorage2 for PhysicalU8_2 {
    type Type<'a> = u8;
    type Storage<'a> = PrimitiveStorageSlice<'a, u8>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::UInt8(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected u8")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalU16_2;

impl PhysicalStorage2 for PhysicalU16_2 {
    type Type<'a> = u16;
    type Storage<'a> = PrimitiveStorageSlice<'a, u16>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::UInt16(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected u16")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalU32_2;

impl PhysicalStorage2 for PhysicalU32_2 {
    type Type<'a> = u32;
    type Storage<'a> = PrimitiveStorageSlice<'a, u32>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::UInt32(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected u32")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalU64_2;

impl PhysicalStorage2 for PhysicalU64_2 {
    type Type<'a> = u64;
    type Storage<'a> = PrimitiveStorageSlice<'a, u64>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::UInt64(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected u64")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalU128_2;

impl PhysicalStorage2 for PhysicalU128_2 {
    type Type<'a> = u128;
    type Storage<'a> = PrimitiveStorageSlice<'a, u128>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::UInt128(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected u128")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalF16_2;

impl PhysicalStorage2 for PhysicalF16_2 {
    type Type<'a> = f16;
    type Storage<'a> = PrimitiveStorageSlice<'a, f16>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Float16(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected f32")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalF32_2;

impl PhysicalStorage2 for PhysicalF32_2 {
    type Type<'a> = f32;
    type Storage<'a> = PrimitiveStorageSlice<'a, f32>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Float32(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected f32")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalF64_2;

impl PhysicalStorage2 for PhysicalF64_2 {
    type Type<'a> = f64;
    type Storage<'a> = PrimitiveStorageSlice<'a, f64>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Float64(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected f64")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalInterval_2;

impl PhysicalStorage2 for PhysicalInterval_2 {
    type Type<'a> = Interval;
    type Storage<'a> = PrimitiveStorageSlice<'a, Interval>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Interval(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected interval")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalBinary_2;

impl PhysicalStorage2 for PhysicalBinary_2 {
    type Type<'a> = &'a [u8];
    type Storage<'a> = BinaryDataStorage<'a>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Binary(binary) => match binary {
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

#[derive(Debug, Clone, Copy)]
pub struct PhysicalUtf8_2;

impl PhysicalStorage2 for PhysicalUtf8_2 {
    type Type<'a> = &'a str;
    type Storage<'a> = StrDataStorage<'a>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::Binary(binary) => match binary {
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

#[derive(Debug, Clone, Copy)]
pub struct PhysicalList_2;

impl PhysicalStorage2 for PhysicalList_2 {
    type Type<'a> = ListItemMetadata;
    type Storage<'a> = PrimitiveStorageSlice<'a, ListItemMetadata>;

    fn get_storage(data: &ArrayData2) -> Result<Self::Storage<'_>> {
        match data {
            ArrayData2::List(storage) => Ok(storage.metadata.as_primitive_storage_slice()),
            _ => Err(RayexecError::new("invalid storage, expected list")),
        }
    }
}