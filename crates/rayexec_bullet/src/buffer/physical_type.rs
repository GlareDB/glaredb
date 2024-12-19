use std::fmt::{self, Debug};

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

    /// The native types being stored in the primary buffers.
    type BufferType: Sized + Debug + Default + Sync + Send + Clone + Copy;

    fn buffer_mem_size() -> usize {
        std::mem::size_of::<Self::BufferType>()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalI8;

impl PhysicalStorage for PhysicalI8 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int8;

    type BufferType = i8;
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalInterval;

impl PhysicalStorage for PhysicalInterval {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Interval;

    type BufferType = Interval;
}
