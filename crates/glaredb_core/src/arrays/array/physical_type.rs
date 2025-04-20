use std::fmt::{self, Debug};

use glaredb_error::{DbError, Result};
use glaredb_proto::ProtoConv;
use half::f16;

use super::Array;
use super::array_buffer::{
    AnyArrayBuffer,
    ArrayBuffer,
    ArrayBuffer2,
    ArrayBufferDowncast,
    ListBuffer,
    ListItemMetadata,
    ScalarBuffer,
    StringBuffer,
    StringViewBuffer,
};
use super::execution_format::ExecutionFormat;
use crate::arrays::array::array_buffer::ArrayBufferType2;
use crate::arrays::scalar::interval::Interval;
use crate::arrays::string::StringView;
use crate::util::convert::TryAsMut;

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
    type ProtoType = glaredb_proto::generated::physical_type::PhysicalType;

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
            Self::ProtoType::InvalidPhysicalType => return Err(DbError::new("invalid")),
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
pub trait Addressable<'a>: Debug {
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
pub trait AddressableMut: Debug {
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
#[derive(Debug, Clone, Copy)]
pub struct PrimitiveSlice<'a, T> {
    pub slice: &'a [T],
}

impl<'a, T> Addressable<'a> for PrimitiveSlice<'a, T>
where
    T: Debug + Send,
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
#[derive(Debug)]
pub struct PrimitiveSliceMut<'a, T> {
    pub slice: &'a mut [T],
}

impl<T> AddressableMut for PrimitiveSliceMut<'_, T>
where
    T: Debug + Send + Copy,
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

pub trait AsSliceMut<T> {
    fn as_slice_mut(&mut self) -> &mut [T];
}

impl<T> AsSliceMut<T> for PrimitiveSliceMut<'_, T> {
    fn as_slice_mut(&mut self) -> &mut [T] {
        self.slice
    }
}

/// Helper trait for getting the underlying data for an array.
pub trait ScalarStorage: Debug + Default + Sync + Send + Clone + Copy + 'static {
    const PHYSICAL_TYPE: PhysicalType;

    /// The logical type being stored that can be accessed.
    ///
    /// For primitive buffers, this will be the same as the primary buffer type.
    type StorageType: Debug + Sync + Send + ?Sized;

    /// The type of the addressable storage.
    type Addressable<'a>: Addressable<'a, T = Self::StorageType>;

    type ArrayBuffer: ArrayBuffer;

    fn buffer_downcast_ref(buffer: &AnyArrayBuffer) -> Result<&Self::ArrayBuffer> {
        ArrayBufferDowncast::downcast_ref(buffer)
    }

    fn downcast_execution_format(
        buffer: &AnyArrayBuffer,
    ) -> Result<ExecutionFormat<'_, Self::ArrayBuffer>> {
        ArrayBufferDowncast::downcast_execution_format(buffer)
    }

    fn addressable(buffer: &Self::ArrayBuffer) -> Self::Addressable<'_>;

    /// Get addressable storage for indexing into the array.
    fn get_addressable(buffer: &AnyArrayBuffer) -> Result<Self::Addressable<'_>>;
}

pub trait MutableScalarStorage: ScalarStorage {
    type AddressableMut<'a>: AddressableMut<T = Self::StorageType>;

    fn buffer_downcast_mut(buffer: &mut AnyArrayBuffer) -> Result<&mut Self::ArrayBuffer> {
        ArrayBufferDowncast::downcast_mut(buffer)
    }

    /// Get mutable addressable storage for the array.
    fn get_addressable_mut(buffer: &mut AnyArrayBuffer) -> Result<Self::AddressableMut<'_>>;
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
            type Addressable<'a> = PrimitiveSlice<'a, Self::StorageType>;

            type ArrayBuffer = ScalarBuffer<$prim>;

            fn addressable(buffer: &Self::ArrayBuffer) -> Self::Addressable<'_> {
                PrimitiveSlice {
                    slice: unsafe { buffer.buffer.as_slice() },
                }
            }

            fn get_addressable(buffer: &AnyArrayBuffer) -> Result<Self::Addressable<'_>> {
                let buf = &Self::buffer_downcast_ref(buffer)?.buffer;
                Ok(PrimitiveSlice {
                    slice: unsafe { buf.as_slice() },
                })
            }
        }

        impl MutableScalarStorage for $name {
            type AddressableMut<'a> = PrimitiveSliceMut<'a, Self::StorageType>;

            fn get_addressable_mut(
                buffer: &mut AnyArrayBuffer,
            ) -> Result<Self::AddressableMut<'_>> {
                let buf = &mut Self::buffer_downcast_mut(buffer)?.buffer;
                Ok(PrimitiveSliceMut {
                    slice: unsafe { buf.as_slice_mut() },
                })
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
pub struct StringViewAddressable<'a> {
    pub(crate) metadata: &'a [StringView],
    pub(crate) buffer: &'a StringViewBuffer,
}

impl<'a> Addressable<'a> for StringViewAddressable<'a> {
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
pub struct StringViewAddressableMut<'a> {
    pub(crate) metadata: &'a mut [StringView],
    pub(crate) buffer: &'a mut StringViewBuffer,
}

impl AddressableMut for StringViewAddressableMut<'_> {
    type T = str;

    fn get_mut(&mut self, idx: usize) -> Option<&mut Self::T> {
        let m = self.metadata.get_mut(idx)?;
        let bs = self.buffer.get_mut(m);

        Some(unsafe { std::str::from_utf8_unchecked_mut(bs) })
    }

    fn put(&mut self, idx: usize, val: &Self::T) {
        let view = self.buffer.push_bytes_as_row(val.as_bytes()).unwrap(); // TODO
        self.metadata[idx] = view;
    }

    fn len(&self) -> usize {
        self.metadata.len()
    }
}

#[derive(Debug)]
pub struct BinaryViewAddressable<'a> {
    pub(crate) metadata: &'a [StringView],
    pub(crate) buffer: &'a StringViewBuffer,
}

impl<'a> Addressable<'a> for BinaryViewAddressable<'a> {
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
pub struct BinaryViewAddressableMut<'a> {
    pub(crate) metadata: &'a mut [StringView],
    pub(crate) buffer: &'a mut StringViewBuffer,
}

impl BinaryViewAddressableMut<'_> {
    /// Clears the underlying binary buffer.
    ///
    /// This does not modify or otherwise zero out the metadata objects.
    /// Metadata must be overwritten with valid data to avoid out-of-bounds
    /// access.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl AddressableMut for BinaryViewAddressableMut<'_> {
    type T = [u8];

    fn get_mut(&mut self, idx: usize) -> Option<&mut Self::T> {
        let m = self.metadata.get_mut(idx)?;
        let bs = self.buffer.get_mut(m);
        Some(bs)
    }

    fn put(&mut self, idx: usize, val: &Self::T) {
        let view = self.buffer.push_bytes_as_row(val).unwrap(); // TODO
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
    type Addressable<'a> = BinaryViewAddressable<'a>;

    type ArrayBuffer = StringBuffer;

    fn addressable(buffer: &Self::ArrayBuffer) -> Self::Addressable<'_> {
        let metadata = unsafe { buffer.metadata.as_slice() };
        BinaryViewAddressable {
            metadata,
            buffer: &buffer.buffer,
        }
    }

    fn get_addressable(buffer: &AnyArrayBuffer) -> Result<Self::Addressable<'_>> {
        let buf = Self::buffer_downcast_ref(buffer)?;
        let metadata = unsafe { buf.metadata.as_slice() };

        Ok(BinaryViewAddressable {
            metadata,
            buffer: &buf.buffer,
        })
    }
}

impl MutableScalarStorage for PhysicalBinary {
    type AddressableMut<'a> = BinaryViewAddressableMut<'a>;

    fn get_addressable_mut(buffer: &mut AnyArrayBuffer) -> Result<Self::AddressableMut<'_>> {
        let buf = Self::buffer_downcast_mut(buffer)?;
        let metadata = unsafe { buf.metadata.as_slice_mut() };

        Ok(BinaryViewAddressableMut {
            metadata,
            buffer: &mut buf.buffer,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PhysicalUtf8;

impl ScalarStorage for PhysicalUtf8 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Utf8;

    type StorageType = str;
    type Addressable<'a> = StringViewAddressable<'a>;

    type ArrayBuffer = StringBuffer;

    fn addressable(buffer: &Self::ArrayBuffer) -> Self::Addressable<'_> {
        let metadata = unsafe { buffer.metadata.as_slice() };
        StringViewAddressable {
            metadata,
            buffer: &buffer.buffer,
        }
    }

    fn get_addressable(buffer: &AnyArrayBuffer) -> Result<Self::Addressable<'_>> {
        let buf = Self::buffer_downcast_ref(buffer)?;
        let metadata = unsafe { buf.metadata.as_slice() };

        Ok(StringViewAddressable {
            metadata,
            buffer: &buf.buffer,
        })
    }
}

impl MutableScalarStorage for PhysicalUtf8 {
    type AddressableMut<'a> = StringViewAddressableMut<'a>;

    fn get_addressable_mut(buffer: &mut AnyArrayBuffer) -> Result<Self::AddressableMut<'_>> {
        let buf = Self::buffer_downcast_mut(buffer)?;
        let metadata = unsafe { buf.metadata.as_slice_mut() };

        Ok(StringViewAddressableMut {
            metadata,
            buffer: &mut buf.buffer,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PhysicalList;

impl ScalarStorage for PhysicalList {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::List;

    type StorageType = ListItemMetadata;
    type Addressable<'a> = PrimitiveSlice<'a, Self::StorageType>;

    type ArrayBuffer = ListBuffer;

    fn addressable(buffer: &Self::ArrayBuffer) -> Self::Addressable<'_> {
        unimplemented!()
    }

    fn get_addressable(buffer: &AnyArrayBuffer) -> Result<Self::Addressable<'_>> {
        unimplemented!()
        // match buffer.as_ref() {
        //     ArrayBufferType2::List(buf) => {
        //         let s = buf.metadata.as_slice();
        //         Ok(PrimitiveSlice { slice: s })
        //     }
        //     _ => Err(DbError::new("invalid buffer type, expected list buffer")),
        // }
    }
}

impl MutableScalarStorage for PhysicalList {
    type AddressableMut<'a> = PrimitiveSliceMut<'a, Self::StorageType>;

    fn get_addressable_mut(buffer: &mut AnyArrayBuffer) -> Result<Self::AddressableMut<'_>> {
        unimplemented!()
        // match buffer.as_mut() {
        //     ArrayBufferType2::List(buf) => {
        //         let s = buf.metadata.try_as_mut()?.as_slice_mut();
        //         Ok(PrimitiveSliceMut { slice: s })
        //     }
        //     _ => Err(DbError::new("invalid buffer type, expected list buffer")),
        // }
    }
}
