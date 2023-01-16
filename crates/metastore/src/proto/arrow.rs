#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Schema {
    #[prost(message, repeated, tag = "1")]
    pub columns: ::prost::alloc::vec::Vec<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Field {
    /// Name of the field.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, boxed, tag = "2")]
    pub arrow_type: ::core::option::Option<::prost::alloc::boxed::Box<ArrowType>>,
    #[prost(bool, tag = "3")]
    pub nullable: bool,
    /// For complex data types like structs, unions.
    #[prost(message, repeated, tag = "4")]
    pub children: ::prost::alloc::vec::Vec<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FixedSizeBinary {
    #[prost(int32, tag = "1")]
    pub length: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    #[prost(enumeration = "TimeUnit", tag = "1")]
    pub time_unit: i32,
    #[prost(string, tag = "2")]
    pub timezone: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Decimal {
    #[prost(uint32, tag = "3")]
    pub precision: u32,
    #[prost(int32, tag = "4")]
    pub scale: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct List {
    #[prost(message, optional, boxed, tag = "1")]
    pub field_type: ::core::option::Option<::prost::alloc::boxed::Box<Field>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FixedSizeList {
    #[prost(message, optional, boxed, tag = "1")]
    pub field_type: ::core::option::Option<::prost::alloc::boxed::Box<Field>>,
    #[prost(int32, tag = "2")]
    pub list_size: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Dictionary {
    #[prost(message, optional, boxed, tag = "1")]
    pub key: ::core::option::Option<::prost::alloc::boxed::Box<ArrowType>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::boxed::Box<ArrowType>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Struct {
    #[prost(message, repeated, tag = "1")]
    pub sub_field_types: ::prost::alloc::vec::Vec<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Union {
    #[prost(message, repeated, tag = "1")]
    pub union_types: ::prost::alloc::vec::Vec<Field>,
    #[prost(enumeration = "UnionMode", tag = "2")]
    pub union_mode: i32,
    #[prost(int32, repeated, tag = "3")]
    pub type_ids: ::prost::alloc::vec::Vec<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarListValue {
    /// encode null explicitly to distinguish a list with a null value
    /// from a list with no values)
    #[prost(bool, tag = "3")]
    pub is_null: bool,
    #[prost(message, optional, tag = "1")]
    pub field: ::core::option::Option<Field>,
    #[prost(message, repeated, tag = "2")]
    pub values: ::prost::alloc::vec::Vec<ScalarValue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarTime32Value {
    #[prost(oneof = "scalar_time32_value::Value", tags = "1, 2")]
    pub value: ::core::option::Option<scalar_time32_value::Value>,
}
/// Nested message and enum types in `ScalarTime32Value`.
pub mod scalar_time32_value {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(int32, tag = "1")]
        Time32SecondValue(i32),
        #[prost(int32, tag = "2")]
        Time32MillisecondValue(i32),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarTime64Value {
    #[prost(oneof = "scalar_time64_value::Value", tags = "1, 2")]
    pub value: ::core::option::Option<scalar_time64_value::Value>,
}
/// Nested message and enum types in `ScalarTime64Value`.
pub mod scalar_time64_value {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(int64, tag = "1")]
        Time64MicrosecondValue(i64),
        #[prost(int64, tag = "2")]
        Time64NanosecondValue(i64),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarTimestampValue {
    #[prost(string, tag = "5")]
    pub timezone: ::prost::alloc::string::String,
    #[prost(oneof = "scalar_timestamp_value::Value", tags = "1, 2, 3, 4")]
    pub value: ::core::option::Option<scalar_timestamp_value::Value>,
}
/// Nested message and enum types in `ScalarTimestampValue`.
pub mod scalar_timestamp_value {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(int64, tag = "1")]
        TimeMicrosecondValue(i64),
        #[prost(int64, tag = "2")]
        TimeNanosecondValue(i64),
        #[prost(int64, tag = "3")]
        TimeSecondValue(i64),
        #[prost(int64, tag = "4")]
        TimeMillisecondValue(i64),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarDictionaryValue {
    #[prost(message, optional, tag = "1")]
    pub index_type: ::core::option::Option<ArrowType>,
    #[prost(message, optional, boxed, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::boxed::Box<ScalarValue>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntervalMonthDayNanoValue {
    #[prost(int32, tag = "1")]
    pub months: i32,
    #[prost(int32, tag = "2")]
    pub days: i32,
    #[prost(int64, tag = "3")]
    pub nanos: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StructValue {
    /// Note that a null struct value must have one or more fields, so we
    /// encode a null StructValue as one witth an empty field_values
    /// list.
    #[prost(message, repeated, tag = "2")]
    pub field_values: ::prost::alloc::vec::Vec<ScalarValue>,
    #[prost(message, repeated, tag = "3")]
    pub fields: ::prost::alloc::vec::Vec<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarFixedSizeBinary {
    #[prost(bytes = "vec", tag = "1")]
    pub values: ::prost::alloc::vec::Vec<u8>,
    #[prost(int32, tag = "2")]
    pub length: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarValue {
    #[prost(
        oneof = "scalar_value::Value",
        tags = "33, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 20, 21, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34"
    )]
    pub value: ::core::option::Option<scalar_value::Value>,
}
/// Nested message and enum types in `ScalarValue`.
pub mod scalar_value {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        /// was PrimitiveScalarType null_value = 19;
        /// Null value of any type
        #[prost(message, tag = "33")]
        NullValue(super::ArrowType),
        #[prost(bool, tag = "1")]
        BoolValue(bool),
        #[prost(string, tag = "2")]
        Utf8Value(::prost::alloc::string::String),
        #[prost(string, tag = "3")]
        LargeUtf8Value(::prost::alloc::string::String),
        #[prost(int32, tag = "4")]
        Int8Value(i32),
        #[prost(int32, tag = "5")]
        Int16Value(i32),
        #[prost(int32, tag = "6")]
        Int32Value(i32),
        #[prost(int64, tag = "7")]
        Int64Value(i64),
        #[prost(uint32, tag = "8")]
        Uint8Value(u32),
        #[prost(uint32, tag = "9")]
        Uint16Value(u32),
        #[prost(uint32, tag = "10")]
        Uint32Value(u32),
        #[prost(uint64, tag = "11")]
        Uint64Value(u64),
        #[prost(float, tag = "12")]
        Float32Value(f32),
        #[prost(double, tag = "13")]
        Float64Value(f64),
        /// Literal Date32 value always has a unit of day
        #[prost(int32, tag = "14")]
        Date32Value(i32),
        #[prost(message, tag = "15")]
        Time32Value(super::ScalarTime32Value),
        /// WAS: ScalarType null_list_value = 18;
        #[prost(message, tag = "17")]
        ListValue(super::ScalarListValue),
        #[prost(message, tag = "20")]
        Decimal128Value(super::Decimal128),
        #[prost(int64, tag = "21")]
        Date64Value(i64),
        #[prost(int32, tag = "24")]
        IntervalYearmonthValue(i32),
        #[prost(int64, tag = "25")]
        IntervalDaytimeValue(i64),
        #[prost(message, tag = "26")]
        TimestampValue(super::ScalarTimestampValue),
        #[prost(message, tag = "27")]
        DictionaryValue(::prost::alloc::boxed::Box<super::ScalarDictionaryValue>),
        #[prost(bytes, tag = "28")]
        BinaryValue(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag = "29")]
        LargeBinaryValue(::prost::alloc::vec::Vec<u8>),
        #[prost(message, tag = "30")]
        Time64Value(super::ScalarTime64Value),
        #[prost(message, tag = "31")]
        IntervalMonthDayNano(super::IntervalMonthDayNanoValue),
        #[prost(message, tag = "32")]
        StructValue(super::StructValue),
        #[prost(message, tag = "34")]
        FixedSizeBinaryValue(super::ScalarFixedSizeBinary),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Decimal128 {
    #[prost(bytes = "vec", tag = "1")]
    pub value: ::prost::alloc::vec::Vec<u8>,
    #[prost(int64, tag = "2")]
    pub p: i64,
    #[prost(int64, tag = "3")]
    pub s: i64,
}
/// Serialized data type
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowType {
    #[prost(
        oneof = "arrow_type::ArrowTypeEnum",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 32, 15, 16, 31, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30"
    )]
    pub arrow_type_enum: ::core::option::Option<arrow_type::ArrowTypeEnum>,
}
/// Nested message and enum types in `ArrowType`.
pub mod arrow_type {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ArrowTypeEnum {
        #[prost(message, tag = "1")]
        None(super::EmptyMessage),
        #[prost(message, tag = "2")]
        Bool(super::EmptyMessage),
        #[prost(message, tag = "3")]
        Uint8(super::EmptyMessage),
        #[prost(message, tag = "4")]
        Int8(super::EmptyMessage),
        #[prost(message, tag = "5")]
        Uint16(super::EmptyMessage),
        #[prost(message, tag = "6")]
        Int16(super::EmptyMessage),
        #[prost(message, tag = "7")]
        Uint32(super::EmptyMessage),
        #[prost(message, tag = "8")]
        Int32(super::EmptyMessage),
        #[prost(message, tag = "9")]
        Uint64(super::EmptyMessage),
        #[prost(message, tag = "10")]
        Int64(super::EmptyMessage),
        #[prost(message, tag = "11")]
        Float16(super::EmptyMessage),
        #[prost(message, tag = "12")]
        Float32(super::EmptyMessage),
        #[prost(message, tag = "13")]
        Float64(super::EmptyMessage),
        #[prost(message, tag = "14")]
        Utf8(super::EmptyMessage),
        #[prost(message, tag = "32")]
        LargeUtf8(super::EmptyMessage),
        #[prost(message, tag = "15")]
        Binary(super::EmptyMessage),
        #[prost(int32, tag = "16")]
        FixedSizeBinary(i32),
        #[prost(message, tag = "31")]
        LargeBinary(super::EmptyMessage),
        #[prost(message, tag = "17")]
        Date32(super::EmptyMessage),
        #[prost(message, tag = "18")]
        Date64(super::EmptyMessage),
        #[prost(enumeration = "super::TimeUnit", tag = "19")]
        Duration(i32),
        #[prost(message, tag = "20")]
        Timestamp(super::Timestamp),
        #[prost(enumeration = "super::TimeUnit", tag = "21")]
        Time32(i32),
        #[prost(enumeration = "super::TimeUnit", tag = "22")]
        Time64(i32),
        #[prost(enumeration = "super::IntervalUnit", tag = "23")]
        Interval(i32),
        #[prost(message, tag = "24")]
        Decimal(super::Decimal),
        #[prost(message, tag = "25")]
        List(::prost::alloc::boxed::Box<super::List>),
        #[prost(message, tag = "26")]
        LargeList(::prost::alloc::boxed::Box<super::List>),
        #[prost(message, tag = "27")]
        FixedSizeList(::prost::alloc::boxed::Box<super::FixedSizeList>),
        #[prost(message, tag = "28")]
        Struct(super::Struct),
        #[prost(message, tag = "29")]
        Union(super::Union),
        #[prost(message, tag = "30")]
        Dictionary(::prost::alloc::boxed::Box<super::Dictionary>),
    }
}
/// Useful for representing an empty enum variant in rust
///   E.G. enum example{One, Two(i32)}
///   maps to
///   message example{
///      oneof{
///          EmptyMessage One = 1;
///          i32 Two = 2;
///     }
/// }
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyMessage {}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DateUnit {
    Day = 0,
    DateMillisecond = 1,
}
impl DateUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DateUnit::Day => "Day",
            DateUnit::DateMillisecond => "DateMillisecond",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Day" => Some(Self::Day),
            "DateMillisecond" => Some(Self::DateMillisecond),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TimeUnit {
    Second = 0,
    Millisecond = 1,
    Microsecond = 2,
    Nanosecond = 3,
}
impl TimeUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TimeUnit::Second => "Second",
            TimeUnit::Millisecond => "Millisecond",
            TimeUnit::Microsecond => "Microsecond",
            TimeUnit::Nanosecond => "Nanosecond",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Second" => Some(Self::Second),
            "Millisecond" => Some(Self::Millisecond),
            "Microsecond" => Some(Self::Microsecond),
            "Nanosecond" => Some(Self::Nanosecond),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum IntervalUnit {
    YearMonth = 0,
    DayTime = 1,
    MonthDayNano = 2,
}
impl IntervalUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            IntervalUnit::YearMonth => "YearMonth",
            IntervalUnit::DayTime => "DayTime",
            IntervalUnit::MonthDayNano => "MonthDayNano",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "YearMonth" => Some(Self::YearMonth),
            "DayTime" => Some(Self::DayTime),
            "MonthDayNano" => Some(Self::MonthDayNano),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum UnionMode {
    Sparse = 0,
    Dense = 1,
}
impl UnionMode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            UnionMode::Sparse => "sparse",
            UnionMode::Dense => "dense",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "sparse" => Some(Self::Sparse),
            "dense" => Some(Self::Dense),
            _ => None,
        }
    }
}
