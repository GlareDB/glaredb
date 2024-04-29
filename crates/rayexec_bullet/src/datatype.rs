use std::hash::Hash;

/// All possible data types.
// TODO: Additional types (compound, decimal, timestamp, etc)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Null,
    Boolean,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
}

impl DataType {
    pub const fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Float32
                | Self::Float64
                | Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
        )
    }
}
