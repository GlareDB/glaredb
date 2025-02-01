use std::fmt::{self, Debug};
use std::marker::PhantomData;

use half::f16;
use rayexec_error::{RayexecError, Result};
use rayexec_proto::ProtoConv;

use super::array_buffer::{ArrayBuffer, ListItemMetadata, StringViewBuffer};
use super::buffer_manager::BufferManager;
use crate::arrays::array::array_buffer::ArrayBufferType;
use crate::arrays::scalar::interval::Interval;
use crate::arrays::view::{StringView, MAX_INLINE_LEN};

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
}

impl PhysicalType {
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
        })
    }
}

/// Represents an in-memory array that can be indexed into to retrieve values.
pub trait Addressable<'a, B: BufferManager>: Debug {
    /// The type that get's returned.
    type T: Send + Debug + ?Sized;

    /// Get a value at the given index.
    fn get(&self, idx: usize) -> Option<&'a Self::T>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Represents in-memory storage that we can get mutable references to.
pub trait AddressableMut<B: BufferManager>: Debug {
    type T: Debug + ?Sized;

    /// Get a mutable reference to a value at the given index.
    fn get_mut(&mut self, idx: usize) -> Option<&mut Self::T>;

    /// Put a value at the given index.
    ///
    /// Should panic if index is out of bounds.
    fn put(&mut self, idx: usize, val: &Self::T);

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Thin wrapper around a slice implementing `Addressable`.
///
/// This mostly exists to ensure the slice is tied to the buffer manager
/// generic.
#[derive(Debug, Clone, Copy)]
pub struct PrimitiveSlice<'a, T, B: BufferManager> {
    pub slice: &'a [T],
    _b: PhantomData<B>,
}

impl<'a, T, B> Addressable<'a, B> for PrimitiveSlice<'a, T, B>
where
    T: Debug + Send,
    B: BufferManager,
{
    type T = T;

    fn get(&self, idx: usize) -> Option<&'a Self::T> {
        self.slice.get(idx)
    }

    fn len(&self) -> usize {
        self.slice.len()
    }
}

/// Thin wrapper around a mutable slice.
///
/// Same rationale as the non-mut struct.
#[derive(Debug)]
pub struct PrimitiveSliceMut<'a, T, B: BufferManager> {
    pub slice: &'a mut [T],
    _b: PhantomData<B>,
}

impl<T, B> AddressableMut<B> for PrimitiveSliceMut<'_, T, B>
where
    T: Debug + Send + Copy,
    B: BufferManager,
{
    type T = T;

    fn get_mut(&mut self, idx: usize) -> Option<&mut Self::T> {
        self.slice.get_mut(idx)
    }

    fn put(&mut self, idx: usize, val: &Self::T) {
        self.slice[idx] = *val
    }

    fn len(&self) -> usize {
        self.slice.len()
    }
}

/// Helper trait for getting the underlying data for an array.
pub trait ScalarStorage: Debug + Default + Sync + Send + Clone + Copy + 'static {
    const PHYSICAL_TYPE: PhysicalType;

    /// The logical type being stored that can be accessed.
    ///
    /// For primitive buffers, this will be the same as the primary buffer type.
    type StorageType: Sync + Send + ?Sized;

    /// The type of the addressable storage.
    type Addressable<'a, B: BufferManager + 'a>: Addressable<'a, B, T = Self::StorageType>;

    /// Get addressable storage for indexing into the array.
    fn get_addressable<B: BufferManager>(
        buffer: &ArrayBuffer<B>,
    ) -> Result<Self::Addressable<'_, B>>;
}

pub trait MutableScalarStorage: ScalarStorage {
    type AddressableMut<'a, B: BufferManager + 'a>: AddressableMut<B, T = Self::StorageType>;

    /// Get mutable addressable storage for the array.
    fn get_addressable_mut<B: BufferManager>(
        buffer: &mut ArrayBuffer<B>,
    ) -> Result<Self::AddressableMut<'_, B>>;

    /// Try to reserve the buffer to hold `addition` number of elements.
    fn try_reserve<B: BufferManager>(buffer: &mut ArrayBuffer<B>, additional: usize) -> Result<()>;
}

/// Marker type representing a null value without an associated type.
///
/// This will be the type we use for queries like `SELECT NULL` where there's no
/// additional type information in the query.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct UntypedNull;

macro_rules! generate_primitive {
    ($prim:ty, $name:ident, $variant:ident) => {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        pub struct $name;

        impl ScalarStorage for $name {
            const PHYSICAL_TYPE: PhysicalType = PhysicalType::$variant;

            type StorageType = $prim;
            type Addressable<'a, B: BufferManager + 'a> = PrimitiveSlice<'a, Self::StorageType, B>;

            fn get_addressable<B: BufferManager>(
                buffer: &ArrayBuffer<B>,
            ) -> Result<Self::Addressable<'_, B>> {
                let s = buffer.get_scalar_buffer()?.try_as_slice::<Self>()?;
                Ok(PrimitiveSlice {
                    slice: s,
                    _b: PhantomData,
                })
            }
        }

        impl MutableScalarStorage for $name {
            type AddressableMut<'a, B: BufferManager + 'a> =
                PrimitiveSliceMut<'a, Self::StorageType, B>;

            fn get_addressable_mut<B: BufferManager>(
                buffer: &mut ArrayBuffer<B>,
            ) -> Result<Self::AddressableMut<'_, B>> {
                let s = buffer.get_scalar_buffer_mut()?.try_as_slice_mut::<Self>()?;
                Ok(PrimitiveSliceMut {
                    slice: s,
                    _b: PhantomData,
                })
            }

            fn try_reserve<B: BufferManager>(
                buffer: &mut ArrayBuffer<B>,
                additional: usize,
            ) -> Result<()> {
                match buffer.as_mut() {
                    ArrayBufferType::Scalar(buf) => buf.try_reserve::<Self>(additional),
                    _ => Err(RayexecError::new(
                        "invalid buffer type, expected scalar buffer",
                    )),
                }
            }
        }
    };
}

generate_primitive!(UntypedNull, PhysicalUntypedNull, UntypedNull);

generate_primitive!(bool, PhysicalBool, Boolean);

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

#[derive(Debug)]
pub struct StringViewAddressable<'a, B: BufferManager> {
    pub(crate) metadata: &'a [StringView],
    pub(crate) buffer: &'a StringViewBuffer<B>,
}

impl<'a, B> Addressable<'a, B> for StringViewAddressable<'a, B>
where
    B: BufferManager,
{
    type T = str;

    fn get(&self, idx: usize) -> Option<&'a Self::T> {
        let m = self.metadata.get(idx)?;
        let bs = self.buffer.get(m);

        Some(unsafe { std::str::from_utf8_unchecked(bs) })
    }

    fn len(&self) -> usize {
        self.metadata.len()
    }
}

#[derive(Debug)]
pub struct StringViewAddressableMut<'a, B: BufferManager> {
    pub(crate) metadata: &'a mut [StringView],
    pub(crate) buffer: &'a mut StringViewBuffer<B>,
}

impl<B> AddressableMut<B> for StringViewAddressableMut<'_, B>
where
    B: BufferManager,
{
    type T = str;

    fn get_mut(&mut self, idx: usize) -> Option<&mut Self::T> {
        let m = self.metadata.get_mut(idx)?;
        let bs = self.buffer.get_mut(m);

        Some(unsafe { std::str::from_utf8_unchecked_mut(bs) })
    }

    fn put(&mut self, idx: usize, val: &Self::T) {
        let view = self.buffer.push_bytes(val.as_bytes()).unwrap(); // TODO
        self.metadata[idx] = view;
    }

    fn len(&self) -> usize {
        self.metadata.len()
    }
}

#[derive(Debug)]
pub struct BinaryViewAddressable<'a, B: BufferManager> {
    pub(crate) metadata: &'a [StringView],
    pub(crate) buffer: &'a StringViewBuffer<B>,
}

impl<'a, B> Addressable<'a, B> for BinaryViewAddressable<'a, B>
where
    B: BufferManager,
{
    type T = [u8];

    fn get(&self, idx: usize) -> Option<&'a Self::T> {
        let m = self.metadata.get(idx)?;
        let bs = self.buffer.get(m);
        Some(bs)
    }

    fn len(&self) -> usize {
        self.metadata.len()
    }
}

#[derive(Debug)]
pub struct BinaryViewAddressableMut<'a, B: BufferManager> {
    pub(crate) metadata: &'a mut [StringView],
    pub(crate) buffer: &'a mut StringViewBuffer<B>,
}

impl<B> AddressableMut<B> for BinaryViewAddressableMut<'_, B>
where
    B: BufferManager,
{
    type T = [u8];

    fn get_mut(&mut self, idx: usize) -> Option<&mut Self::T> {
        let m = self.metadata.get_mut(idx)?;
        let bs = self.buffer.get_mut(m);
        Some(bs)
    }

    fn put(&mut self, idx: usize, val: &Self::T) {
        let view = self.buffer.push_bytes(val).unwrap(); // TODO
        self.metadata[idx] = view;
    }

    fn len(&self) -> usize {
        self.metadata.len()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PhysicalBinary;

impl ScalarStorage for PhysicalBinary {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Binary;

    type StorageType = [u8];
    type Addressable<'a, B: BufferManager + 'a> = BinaryViewAddressable<'a, B>;

    fn get_addressable<B: BufferManager>(
        buffer: &ArrayBuffer<B>,
    ) -> Result<Self::Addressable<'_, B>> {
        match buffer.as_ref() {
            ArrayBufferType::String(buf) => Ok(buf.as_binary_view()),
            _ => Err(RayexecError::new(
                "invalid buffer type, expected string buffer",
            )),
        }
    }
}

impl MutableScalarStorage for PhysicalBinary {
    type AddressableMut<'a, B: BufferManager + 'a> = BinaryViewAddressableMut<'a, B>;

    fn get_addressable_mut<B: BufferManager>(
        buffer: &mut ArrayBuffer<B>,
    ) -> Result<Self::AddressableMut<'_, B>> {
        match buffer.as_mut() {
            ArrayBufferType::String(buf) => buf.try_as_binary_view_mut(),
            _ => Err(RayexecError::new(
                "invalid buffer type, expected string buffer",
            )),
        }
    }

    fn try_reserve<B: BufferManager>(buffer: &mut ArrayBuffer<B>, additional: usize) -> Result<()> {
        match buffer.as_mut() {
            ArrayBufferType::String(buf) => buf.try_reserve(additional),
            _ => Err(RayexecError::new(
                "invalid buffer type, expected string buffer",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PhysicalUtf8;

impl ScalarStorage for PhysicalUtf8 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Utf8;

    type StorageType = str;
    type Addressable<'a, B: BufferManager + 'a> = StringViewAddressable<'a, B>;

    fn get_addressable<B: BufferManager>(
        buffer: &ArrayBuffer<B>,
    ) -> Result<Self::Addressable<'_, B>> {
        match buffer.as_ref() {
            ArrayBufferType::String(buf) => buf.try_as_string_view(),
            _ => Err(RayexecError::new(
                "invalid buffer type, expected string buffer",
            )),
        }
    }
}

impl MutableScalarStorage for PhysicalUtf8 {
    type AddressableMut<'a, B: BufferManager + 'a> = StringViewAddressableMut<'a, B>;

    fn get_addressable_mut<B: BufferManager>(
        buffer: &mut ArrayBuffer<B>,
    ) -> Result<Self::AddressableMut<'_, B>> {
        match buffer.as_mut() {
            ArrayBufferType::String(buf) => buf.try_as_string_view_mut(),
            _ => Err(RayexecError::new(
                "invalid buffer type, expected string buffer",
            )),
        }
    }

    fn try_reserve<B: BufferManager>(buffer: &mut ArrayBuffer<B>, additional: usize) -> Result<()> {
        match buffer.as_mut() {
            ArrayBufferType::String(buf) => buf.try_reserve(additional),
            _ => Err(RayexecError::new(
                "invalid buffer type, expected string buffer",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PhysicalList;

// TODO: Is this useful?
impl ScalarStorage for PhysicalList {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::List;

    type StorageType = ListItemMetadata;
    type Addressable<'a, B: BufferManager + 'a> = PrimitiveSlice<'a, Self::StorageType, B>;

    fn get_addressable<B: BufferManager>(
        buffer: &ArrayBuffer<B>,
    ) -> Result<Self::Addressable<'_, B>> {
        match buffer.as_ref() {
            ArrayBufferType::List(buf) => {
                let s = buf.metadata.as_slice();
                Ok(PrimitiveSlice {
                    slice: s,
                    _b: PhantomData,
                })
            }
            _ => Err(RayexecError::new(
                "invalid buffer type, expected list buffer",
            )),
        }
    }
}
