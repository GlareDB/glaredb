use std::fmt::{self, Debug};

use super::addressable::{AddressableStorage, MutableAddressableStorage};
use super::string_view::{StringViewBuffer, StringViewMetadataUnion};
use crate::scalar::interval::Interval;

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
}

impl PhysicalType {
    pub fn buffer_mem_size(&self) -> usize {
        match self {
            Self::Int8 => PhysicalI8::buffer_mem_size(),
            Self::Interval => PhysicalInterval::buffer_mem_size(),
            _ => unimplemented!(),
        }
    }

    pub fn as_str(&self) -> &'static str {
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
        }
    }
}

impl fmt::Display for PhysicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub trait PhysicalStorage: Debug + Sync + Send + Clone + Copy + 'static {
    const PHYSICAL_TYPE: PhysicalType;

    /// The types being stored in the primary buffer.
    type PrimaryBufferType: Sized + Debug + Default + Sync + Send + Clone + Copy;

    /// The native value type being stored.
    ///
    /// For many storage types, this is the same as `BufferType`.
    type StorageType: ?Sized;

    type Storage<'a>: AddressableStorage<T = Self::StorageType>;

    fn buffer_mem_size() -> usize {
        std::mem::size_of::<Self::PrimaryBufferType>()
    }
}

pub trait MutablePhysicalStorage: PhysicalStorage {
    type MutableStorage<'a>: MutableAddressableStorage<T = Self::StorageType>;
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalI8;

impl PhysicalStorage for PhysicalI8 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int8;

    type PrimaryBufferType = i8;
    type StorageType = Self::PrimaryBufferType;

    type Storage<'a> = &'a [Self::StorageType];
}

impl MutablePhysicalStorage for PhysicalI8 {
    type MutableStorage<'a> = &'a mut [Self::StorageType];
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalInterval;

impl PhysicalStorage for PhysicalInterval {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Interval;

    type PrimaryBufferType = Interval;
    type StorageType = Self::PrimaryBufferType;

    type Storage<'a> = &'a [Self::StorageType];
}

impl MutablePhysicalStorage for PhysicalInterval {
    type MutableStorage<'a> = &'a mut [Self::StorageType];
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalUtf8;

impl PhysicalStorage for PhysicalUtf8 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Utf8;

    type PrimaryBufferType = StringViewMetadataUnion;
    type StorageType = str;

    type Storage<'a> = StringViewBuffer<'a>;
}
