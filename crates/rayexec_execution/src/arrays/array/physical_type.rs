use std::fmt::{self, Debug};

use half::f16;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_proto::ProtoConv;

use super::array_buffer::{ArrayBuffer, ListItemMetadata};
use super::buffer_manager::BufferManager;
use super::string_view::{
    BinaryViewAddressable,
    BinaryViewAddressableMut,
    StringViewAddressable,
    StringViewAddressableMut,
    StringViewMetadataUnion,
};
use crate::arrays::array::{Array, ArrayData2, BinaryData};
use crate::arrays::executor::builder::{
    ArrayDataBuffer,
    BooleanBuffer,
    GermanVarlenBuffer,
    PrimitiveBuffer,
};
use crate::arrays::scalar::interval::Interval;
use crate::arrays::storage::{
    AddressableStorage,
    BooleanStorageRef,
    ContiguousVarlenStorageSlice,
    GermanVarlenStorageSlice,
    ListItemMetadata2,
    ListStorage,
    PrimitiveStorageSlice,
    UntypedNull2,
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
    List,
    Struct,
    Dictionary,
    Constant,
}

impl PhysicalType {
    // TODO: Remove
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
                metadata: vec![ListItemMetadata2::default(); len].into(),
                array: Array::new_untyped_null_array(0),
            }
            .into(),
            _ => unimplemented!(),
        }
    }

    /// Get the size in bytes of the type that gets placed into the primary
    /// buffer.
    pub const fn primary_buffer_mem_size(&self) -> usize {
        match self {
            Self::UntypedNull => PhysicalUntypedNull::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Boolean => PhysicalBool::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Int8 => PhysicalI8::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Int16 => PhysicalI16::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Int32 => PhysicalI32::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Int64 => PhysicalI64::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Int128 => PhysicalI128::PRIMARY_BUFFER_TYPE_SIZE,
            Self::UInt8 => PhysicalU8::PRIMARY_BUFFER_TYPE_SIZE,
            Self::UInt16 => PhysicalU16::PRIMARY_BUFFER_TYPE_SIZE,
            Self::UInt32 => PhysicalU32::PRIMARY_BUFFER_TYPE_SIZE,
            Self::UInt64 => PhysicalU64::PRIMARY_BUFFER_TYPE_SIZE,
            Self::UInt128 => PhysicalU128::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Float16 => PhysicalF16::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Float32 => PhysicalF32::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Float64 => PhysicalF64::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Interval => PhysicalInterval::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Utf8 => PhysicalUtf8::PRIMARY_BUFFER_TYPE_SIZE,
            Self::List => PhysicalList::PRIMARY_BUFFER_TYPE_SIZE,
            Self::Dictionary => PhysicalDictionary::PRIMARY_BUFFER_TYPE_SIZE,

            // TODO: Struct should zero.
            _ => unimplemented!(),
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::UntypedNull => "UntypedNull",
            Self::Boolean => "Boolean",
            Self::Int8 => "Int8",
            Self::Int16 => "Int16",
            Self::Int32 => "Int32",
            Self::Int64 => "Int64",
            Self::Int128 => "Int128",
            Self::UInt8 => "UInt8",
            Self::UInt16 => "UInt16",
            Self::UInt32 => "UInt32",
            Self::UInt64 => "UInt64",
            Self::UInt128 => "UInt128",
            Self::Float16 => "Float16",
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Interval => "Interval",
            Self::Binary => "Binary",
            Self::Utf8 => "Utf8",
            Self::List => "List",
            Self::Struct => "Struct",
            Self::Dictionary => "Dictionary",
            Self::Constant => "Constant",
        }
    }
}

impl fmt::Display for PhysicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl ProtoConv for PhysicalType {
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
            Self::Struct => Self::ProtoType::Struct,
            Self::Dictionary => Self::ProtoType::Dictionary,
            Self::Constant => Self::ProtoType::Constant,
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
            Self::ProtoType::Struct => Self::Struct,
            Self::ProtoType::Dictionary => Self::Dictionary,
            Self::ProtoType::Constant => Self::Constant,
        })
    }
}

/// Represents an in-memory array that can be indexed into to retrieve values.
pub trait Addressable<'a>: Debug {
    /// The type that get's returned.
    type T: Send + Debug + ?Sized;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a value at the given index.
    fn get(&self, idx: usize) -> Option<&'a Self::T>;
}

impl<'a, T> Addressable<'a> for &'a [T]
where
    T: Debug + Send,
{
    type T = T;

    fn len(&self) -> usize {
        (**self).len()
    }

    fn get(&self, idx: usize) -> Option<&'a Self::T> {
        (**self).get(idx)
    }
}

/// Represents in-memory storage that we can get mutable references to.
pub trait AddressableMut: Debug {
    type T: Debug + ?Sized;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a mutable reference to a value at the given index.
    fn get_mut(&mut self, idx: usize) -> Option<&mut Self::T>;

    /// Put a value at the given index.
    ///
    /// Should panic if index is out of bounds.
    fn put(&mut self, idx: usize, val: &Self::T);
}

impl<T> AddressableMut for &mut [T]
where
    T: Debug + Send + Copy,
{
    type T = T;

    fn len(&self) -> usize {
        (**self).len()
    }

    fn get_mut(&mut self, idx: usize) -> Option<&mut Self::T> {
        (**self).get_mut(idx)
    }

    fn put(&mut self, idx: usize, val: &Self::T) {
        self[idx] = *val;
    }
}

/// Helper trait for getting the underlying data for an array.
pub trait PhysicalStorage: Debug + Default + Sync + Send + Clone + Copy + 'static {
    const PHYSICAL_TYPE: PhysicalType;

    /// Size in bytes of the type being stored in the primary buffer.
    const PRIMARY_BUFFER_TYPE_SIZE: usize = std::mem::size_of::<Self::PrimaryBufferType>();

    /// The type that's stored in the primary buffer.
    ///
    /// This should be small and fixed sized.
    type PrimaryBufferType: Sized + Debug + Default + Sync + Send + Clone + Copy;

    /// The logical type being stored that can be accessed.
    ///
    /// For primitive buffers, this will be the same as the primary buffer type.
    type StorageType: Sync + Send + ?Sized;

    /// The type of the addressable storage.
    type Addressable<'a>: Addressable<'a, T = Self::StorageType>;

    /// Get addressable storage for indexing into the array.
    fn get_addressable<B: BufferManager>(buffer: &ArrayBuffer<B>) -> Result<Self::Addressable<'_>>;
}

pub trait MutablePhysicalStorage: PhysicalStorage {
    type AddressableMut<'a>: AddressableMut<T = Self::StorageType>;

    /// Get mutable addressable storage for the array.
    fn get_addressable_mut<B: BufferManager>(
        buffer: &mut ArrayBuffer<B>,
    ) -> Result<Self::AddressableMut<'_>>;
}

/// Marker type representing a null value without an associated type.
///
/// This will be the type we use for queries like `SELECT NULL` where there's no
/// additional type information in the query.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct UntypedNull;

#[derive(Debug, Clone, Copy, Default)]
pub struct PhysicalUntypedNull;

impl PhysicalStorage for PhysicalUntypedNull {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::UntypedNull;

    type PrimaryBufferType = UntypedNull;
    type StorageType = UntypedNull;

    type Addressable<'a> = &'a [UntypedNull];

    fn get_addressable<B: BufferManager>(buffer: &ArrayBuffer<B>) -> Result<Self::Addressable<'_>> {
        buffer.try_as_slice::<Self>()
    }
}

impl MutablePhysicalStorage for PhysicalUntypedNull {
    type AddressableMut<'a> = &'a mut [UntypedNull];

    fn get_addressable_mut<B: BufferManager>(
        buffer: &mut ArrayBuffer<B>,
    ) -> Result<Self::AddressableMut<'_>> {
        buffer.try_as_slice_mut::<Self>()
    }
}

// TODO: Use primitive macro
#[derive(Debug, Clone, Copy, Default)]
pub struct PhysicalBool;

impl PhysicalStorage for PhysicalBool {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Boolean;

    type PrimaryBufferType = bool;
    type StorageType = bool;

    type Addressable<'a> = &'a [bool];

    fn get_addressable<B: BufferManager>(buffer: &ArrayBuffer<B>) -> Result<Self::Addressable<'_>> {
        buffer.try_as_slice::<Self>()
    }
}

impl MutablePhysicalStorage for PhysicalBool {
    type AddressableMut<'a> = &'a mut [bool];

    fn get_addressable_mut<B: BufferManager>(
        buffer: &mut ArrayBuffer<B>,
    ) -> Result<Self::AddressableMut<'_>> {
        buffer.try_as_slice_mut::<Self>()
    }
}

macro_rules! generate_primitive {
    ($prim:ty, $name:ident, $variant:ident) => {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        pub struct $name;

        impl PhysicalStorage for $name {
            const PHYSICAL_TYPE: PhysicalType = PhysicalType::$variant;

            type PrimaryBufferType = $prim;
            type StorageType = Self::PrimaryBufferType;
            type Addressable<'a> = &'a [Self::StorageType];

            fn get_addressable<B: BufferManager>(
                buffer: &ArrayBuffer<B>,
            ) -> Result<Self::Addressable<'_>> {
                buffer.try_as_slice::<Self>()
            }
        }

        impl MutablePhysicalStorage for $name {
            type AddressableMut<'a> = &'a mut [Self::StorageType];

            fn get_addressable_mut<B: BufferManager>(
                buffer: &mut ArrayBuffer<B>,
            ) -> Result<Self::AddressableMut<'_>> {
                buffer.try_as_slice_mut::<Self>()
            }
        }
    };
}

generate_primitive!(i8, PhysicalI8, Int8);
generate_primitive!(i16, PhysicalI16, Int16);
generate_primitive!(i32, PhysicalI32, Int32);
generate_primitive!(i64, PhysicalI64, Int64);
generate_primitive!(i128, PhysicalI128, Int128);

generate_primitive!(u8, PhysicalU8, UInt8);
generate_primitive!(u16, PhysicalU16, UInt16);
generate_primitive!(u32, PhysicalU32, UInt32);
generate_primitive!(u64, PhysicalU64, UInt64);
generate_primitive!(u128, PhysicalU128, UInt128);

generate_primitive!(f16, PhysicalF16, Float16);
generate_primitive!(f32, PhysicalF32, Float32);
generate_primitive!(f64, PhysicalF64, Float64);

generate_primitive!(Interval, PhysicalInterval, Interval);

#[derive(Debug, Clone, Copy, Default)]
pub struct PhysicalBinary;

impl PhysicalStorage for PhysicalBinary {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Binary;

    type PrimaryBufferType = StringViewMetadataUnion;
    type StorageType = [u8];

    type Addressable<'a> = BinaryViewAddressable<'a>;

    fn get_addressable<B: BufferManager>(buffer: &ArrayBuffer<B>) -> Result<Self::Addressable<'_>> {
        buffer.try_as_binary_view_addressable()
    }
}

impl MutablePhysicalStorage for PhysicalBinary {
    type AddressableMut<'a> = BinaryViewAddressableMut<'a>;

    fn get_addressable_mut<B: BufferManager>(
        buffer: &mut ArrayBuffer<B>,
    ) -> Result<Self::AddressableMut<'_>> {
        buffer.try_as_binary_view_addressable_mut()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PhysicalUtf8;

impl PhysicalStorage for PhysicalUtf8 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Utf8;

    type PrimaryBufferType = StringViewMetadataUnion;
    type StorageType = str;

    type Addressable<'a> = StringViewAddressable<'a>;

    fn get_addressable<B: BufferManager>(buffer: &ArrayBuffer<B>) -> Result<Self::Addressable<'_>> {
        buffer.try_as_string_view_addressable()
    }
}

impl MutablePhysicalStorage for PhysicalUtf8 {
    type AddressableMut<'a> = StringViewAddressableMut<'a>;

    fn get_addressable_mut<B: BufferManager>(
        buffer: &mut ArrayBuffer<B>,
    ) -> Result<Self::AddressableMut<'_>> {
        buffer.try_as_string_view_addressable_mut()
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

#[derive(Debug, Clone, Copy, Default)]
pub struct PhysicalList;

impl PhysicalStorage for PhysicalList {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::List;

    type PrimaryBufferType = ListItemMetadata;
    type StorageType = Self::PrimaryBufferType;

    type Addressable<'a> = &'a [Self::StorageType];

    fn get_addressable<B: BufferManager>(buffer: &ArrayBuffer<B>) -> Result<Self::Addressable<'_>> {
        buffer.try_as_slice::<Self>()
    }
}

impl MutablePhysicalStorage for PhysicalList {
    type AddressableMut<'a> = &'a mut [Self::StorageType];

    fn get_addressable_mut<B: BufferManager>(
        buffer: &mut ArrayBuffer<B>,
    ) -> Result<Self::AddressableMut<'_>> {
        buffer.try_as_slice_mut::<Self>()
    }
}

/// Dictionary arrays have the selection vector as the primary data buffer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalDictionary;

impl PhysicalStorage for PhysicalDictionary {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Dictionary;

    type PrimaryBufferType = usize; // The index into the dictionary.
    type StorageType = Self::PrimaryBufferType;

    type Addressable<'a> = &'a [usize];

    fn get_addressable<B: BufferManager>(buffer: &ArrayBuffer<B>) -> Result<Self::Addressable<'_>> {
        buffer.try_as_slice::<Self>()
    }
}

/// Zero-sized marker value that gets stored in the primary data buffer for a
/// constant array.
///
/// This is used to allow the data buffer have a length, but not actually
/// allocate any storage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConstantMarker;

/// Constant arrays have a single value in the secondary data buffer. The
/// primary data buffer is a slice of `ConstantMarker` to indicate the length of
/// the array.
#[derive(Debug, Default, Clone, Copy)]
pub struct PhysicalConstant;

impl PhysicalStorage for PhysicalConstant {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Constant;

    type PrimaryBufferType = ConstantMarker;
    type StorageType = Self::PrimaryBufferType;

    type Addressable<'a> = &'a [ConstantMarker];

    fn get_addressable<B: BufferManager>(buffer: &ArrayBuffer<B>) -> Result<Self::Addressable<'_>> {
        buffer.try_as_slice::<Self>()
    }
}
