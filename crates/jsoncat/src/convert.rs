//! Type conversions between reexported Arrow types and local Arrow types.
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionMode};
use datafusion::arrow::datatypes::{
    DataType as DfDataType, Field as DfField, IntervalUnit as DfIntervalUnit, Schema as DfSchema,
    TimeUnit as DfTimeUnit, UnionMode as DfUnionMode,
};

pub fn from_df_schema(schema: DfSchema) -> Schema {
    Schema::new(schema.fields.into_iter().map(from_df_field).collect())
}

/// Convert the datatype that's reexported from datafusion to the local arrow
/// crate's datatype.
///
/// This is done because we need the 'serde' feature enabled for the arrow
/// types, and you can't enable features for transitive dependencies.
pub fn from_df_datatype(df: DfDataType) -> DataType {
    match df {
        DfDataType::Null => DataType::Null,
        DfDataType::Boolean => DataType::Boolean,
        DfDataType::Int8 => DataType::Int8,
        DfDataType::Int16 => DataType::Int16,
        DfDataType::Int32 => DataType::Int32,
        DfDataType::Int64 => DataType::Int64,
        DfDataType::UInt8 => DataType::UInt8,
        DfDataType::UInt16 => DataType::UInt16,
        DfDataType::UInt32 => DataType::UInt32,
        DfDataType::UInt64 => DataType::UInt64,
        DfDataType::Float16 => DataType::Float16,
        DfDataType::Float32 => DataType::Float32,
        DfDataType::Float64 => DataType::Float64,
        DfDataType::Timestamp(a, b) => DataType::Timestamp(from_df_timeunit(a), b),
        DfDataType::Date32 => DataType::Date32,
        DfDataType::Date64 => DataType::Date64,
        DfDataType::Time32(a) => DataType::Time32(from_df_timeunit(a)),
        DfDataType::Time64(a) => DataType::Time64(from_df_timeunit(a)),
        DfDataType::Duration(a) => DataType::Duration(from_df_timeunit(a)),
        DfDataType::Interval(a) => DataType::Interval(from_df_intervalunit(a)),
        DfDataType::Binary => DataType::Binary,
        DfDataType::FixedSizeBinary(a) => DataType::FixedSizeBinary(a),
        DfDataType::LargeBinary => DataType::LargeBinary,
        DfDataType::Utf8 => DataType::Utf8,
        DfDataType::LargeUtf8 => DataType::LargeUtf8,
        DfDataType::List(a) => DataType::List(Box::new(from_df_field(*a))),
        DfDataType::FixedSizeList(a, b) => DataType::FixedSizeList(Box::new(from_df_field(*a)), b),
        DfDataType::LargeList(a) => DataType::LargeList(Box::new(from_df_field(*a))),
        DfDataType::Struct(a) => DataType::Struct(a.into_iter().map(from_df_field).collect()),
        DfDataType::Union(a, b, c) => DataType::Union(
            a.into_iter().map(from_df_field).collect(),
            b,
            from_df_union_mode(c),
        ),
        DfDataType::Dictionary(a, b) => DataType::Dictionary(
            Box::new(from_df_datatype(*a)),
            Box::new(from_df_datatype(*b)),
        ),
        DfDataType::Decimal128(a, b) => DataType::Decimal128(a, b as u8),
        DfDataType::Decimal256(a, b) => DataType::Decimal256(a, b as u8),
        DfDataType::Map(a, b) => DataType::Map(Box::new(from_df_field(*a)), b),
    }
}

fn from_df_timeunit(unit: DfTimeUnit) -> TimeUnit {
    match unit {
        DfTimeUnit::Second => TimeUnit::Second,
        DfTimeUnit::Millisecond => TimeUnit::Millisecond,
        DfTimeUnit::Microsecond => TimeUnit::Microsecond,
        DfTimeUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

fn from_df_intervalunit(unit: DfIntervalUnit) -> IntervalUnit {
    match unit {
        DfIntervalUnit::DayTime => IntervalUnit::DayTime,
        DfIntervalUnit::YearMonth => IntervalUnit::YearMonth,
        DfIntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
    }
}

fn from_df_field(field: DfField) -> Field {
    Field::new(
        field.name(),
        from_df_datatype(field.data_type().clone()),
        field.is_nullable(),
    )
}

fn from_df_union_mode(mode: DfUnionMode) -> UnionMode {
    match mode {
        DfUnionMode::Dense => UnionMode::Dense,
        DfUnionMode::Sparse => UnionMode::Sparse,
    }
}
