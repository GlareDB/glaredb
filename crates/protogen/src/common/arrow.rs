//! Arrow type conversions.
//!
//! Note this uses the re-exported Arrow types from Datafusion.
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType,
    Field,
    IntervalUnit,
    TimeUnit,
    UnionFields,
    UnionMode,
};

use super::super::{FromOptionalField, ProtoConvError};
use crate::gen::common::arrow;

impl TryFrom<&arrow::ArrowType> for DataType {
    type Error = ProtoConvError;

    fn try_from(arrow_type: &arrow::ArrowType) -> Result<Self, Self::Error> {
        arrow_type
            .arrow_type_enum
            .as_ref()
            .required("arrow_type_enum")
    }
}

impl TryFrom<&arrow::arrow_type::ArrowTypeEnum> for DataType {
    type Error = ProtoConvError;
    fn try_from(arrow_type_enum: &arrow::arrow_type::ArrowTypeEnum) -> Result<Self, Self::Error> {
        use arrow::arrow_type;
        Ok(match arrow_type_enum {
            arrow_type::ArrowTypeEnum::None(_) => DataType::Null,
            arrow_type::ArrowTypeEnum::Bool(_) => DataType::Boolean,
            arrow_type::ArrowTypeEnum::Uint8(_) => DataType::UInt8,
            arrow_type::ArrowTypeEnum::Int8(_) => DataType::Int8,
            arrow_type::ArrowTypeEnum::Uint16(_) => DataType::UInt16,
            arrow_type::ArrowTypeEnum::Int16(_) => DataType::Int16,
            arrow_type::ArrowTypeEnum::Uint32(_) => DataType::UInt32,
            arrow_type::ArrowTypeEnum::Int32(_) => DataType::Int32,
            arrow_type::ArrowTypeEnum::Uint64(_) => DataType::UInt64,
            arrow_type::ArrowTypeEnum::Int64(_) => DataType::Int64,
            arrow_type::ArrowTypeEnum::Float16(_) => DataType::Float16,
            arrow_type::ArrowTypeEnum::Float32(_) => DataType::Float32,
            arrow_type::ArrowTypeEnum::Float64(_) => DataType::Float64,
            arrow_type::ArrowTypeEnum::Utf8(_) => DataType::Utf8,
            arrow_type::ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            arrow_type::ArrowTypeEnum::Binary(_) => DataType::Binary,
            arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
            arrow_type::ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
            arrow_type::ArrowTypeEnum::Date32(_) => DataType::Date32,
            arrow_type::ArrowTypeEnum::Date64(_) => DataType::Date64,
            arrow_type::ArrowTypeEnum::Duration(time_unit) => {
                DataType::Duration(parse_i32_to_time_unit(time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Timestamp(arrow::Timestamp {
                time_unit,
                timezone,
            }) => DataType::Timestamp(
                parse_i32_to_time_unit(time_unit)?,
                match timezone.len() {
                    0 => None,
                    _ => Some(timezone.as_str().into()),
                },
            ),
            arrow_type::ArrowTypeEnum::Time32(time_unit) => {
                DataType::Time32(parse_i32_to_time_unit(time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Time64(time_unit) => {
                DataType::Time64(parse_i32_to_time_unit(time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Interval(interval_unit) => {
                DataType::Interval(parse_i32_to_interval_unit(interval_unit)?)
            }
            arrow_type::ArrowTypeEnum::Decimal(arrow::Decimal { precision, scale }) => {
                DataType::Decimal128(*precision as u8, *scale as i8)
            }
            arrow_type::ArrowTypeEnum::List(list) => {
                let list_type = list.as_ref().field_type.as_deref().required("field_type")?;
                DataType::List(Arc::new(list_type))
            }
            arrow_type::ArrowTypeEnum::LargeList(list) => {
                let list_type = list.as_ref().field_type.as_deref().required("field_type")?;
                DataType::LargeList(Arc::new(list_type))
            }
            arrow_type::ArrowTypeEnum::FixedSizeList(list) => {
                let list_type = list.as_ref().field_type.as_deref().required("field_type")?;
                let list_size = list.list_size;
                DataType::FixedSizeList(Arc::new(list_type), list_size)
            }
            arrow_type::ArrowTypeEnum::Struct(strct) => DataType::Struct(
                strct
                    .sub_field_types
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<_, _>>()?,
            ),
            arrow_type::ArrowTypeEnum::Union(union) => {
                let union_mode = arrow::UnionMode::try_from(union.union_mode).map_err(|_| {
                    ProtoConvError::UnknownEnumVariant("UnionMode", union.union_mode)
                })?;
                let union_mode = match union_mode {
                    arrow::UnionMode::Dense => UnionMode::Dense,
                    arrow::UnionMode::Sparse => UnionMode::Sparse,
                };
                let union_fields = union
                    .union_types
                    .iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<Field>, _>>()?;

                // Default to index based type ids if not provided
                let type_ids: Vec<_> = match union.type_ids.is_empty() {
                    true => (0..union_fields.len() as i8).collect(),
                    false => union.type_ids.iter().map(|i| *i as i8).collect(),
                };

                DataType::Union(UnionFields::new(type_ids, union_fields), union_mode)
            }
            arrow_type::ArrowTypeEnum::Dictionary(dict) => {
                let key_datatype = dict.as_ref().key.as_deref().required("key")?;
                let value_datatype = dict.as_ref().value.as_deref().required("value")?;
                DataType::Dictionary(Box::new(key_datatype), Box::new(value_datatype))
            }
        })
    }
}

impl TryFrom<&DataType> for arrow::ArrowType {
    type Error = ProtoConvError;

    fn try_from(val: &DataType) -> Result<Self, Self::Error> {
        Ok(Self {
            arrow_type_enum: Some(val.try_into()?),
        })
    }
}

impl TryFrom<&DataType> for arrow::arrow_type::ArrowTypeEnum {
    type Error = ProtoConvError;

    fn try_from(val: &DataType) -> Result<Self, Self::Error> {
        use arrow::EmptyMessage;
        let res = match val {
            DataType::Null => Self::None(EmptyMessage {}),
            DataType::Boolean => Self::Bool(EmptyMessage {}),
            DataType::Int8 => Self::Int8(EmptyMessage {}),
            DataType::Int16 => Self::Int16(EmptyMessage {}),
            DataType::Int32 => Self::Int32(EmptyMessage {}),
            DataType::Int64 => Self::Int64(EmptyMessage {}),
            DataType::UInt8 => Self::Uint8(EmptyMessage {}),
            DataType::UInt16 => Self::Uint16(EmptyMessage {}),
            DataType::UInt32 => Self::Uint32(EmptyMessage {}),
            DataType::UInt64 => Self::Uint64(EmptyMessage {}),
            DataType::Float16 => Self::Float16(EmptyMessage {}),
            DataType::Float32 => Self::Float32(EmptyMessage {}),
            DataType::Float64 => Self::Float64(EmptyMessage {}),
            DataType::Timestamp(time_unit, timezone) => Self::Timestamp(arrow::Timestamp {
                time_unit: arrow::TimeUnit::from(time_unit) as i32,
                timezone: timezone.as_deref().unwrap_or("").to_string(),
            }),
            DataType::Date32 => Self::Date32(EmptyMessage {}),
            DataType::Date64 => Self::Date64(EmptyMessage {}),
            DataType::Time32(time_unit) => Self::Time32(arrow::TimeUnit::from(time_unit) as i32),
            DataType::Time64(time_unit) => Self::Time64(arrow::TimeUnit::from(time_unit) as i32),
            DataType::Duration(time_unit) => {
                Self::Duration(arrow::TimeUnit::from(time_unit) as i32)
            }
            DataType::Interval(interval_unit) => {
                Self::Interval(arrow::IntervalUnit::from(interval_unit) as i32)
            }
            DataType::Binary => Self::Binary(EmptyMessage {}),
            DataType::FixedSizeBinary(size) => Self::FixedSizeBinary(*size),
            DataType::LargeBinary => Self::LargeBinary(EmptyMessage {}),
            DataType::Utf8 => Self::Utf8(EmptyMessage {}),
            DataType::LargeUtf8 => Self::LargeUtf8(EmptyMessage {}),
            DataType::List(item_type) => Self::List(Box::new(arrow::List {
                field_type: Some(Box::new(item_type.as_ref().try_into()?)),
            })),
            DataType::FixedSizeList(item_type, size) => {
                Self::FixedSizeList(Box::new(arrow::FixedSizeList {
                    field_type: Some(Box::new(item_type.as_ref().try_into()?)),
                    list_size: *size,
                }))
            }
            DataType::LargeList(item_type) => Self::LargeList(Box::new(arrow::List {
                field_type: Some(Box::new(item_type.as_ref().try_into()?)),
            })),
            DataType::Struct(struct_fields) => Self::Struct(arrow::Struct {
                sub_field_types: struct_fields
                    .iter()
                    .map(|field| field.as_ref().try_into())
                    .collect::<Result<Vec<_>, ProtoConvError>>()?,
            }),
            DataType::Union(fields, union_mode) => {
                let union_mode = match union_mode {
                    UnionMode::Sparse => arrow::UnionMode::Sparse,
                    UnionMode::Dense => arrow::UnionMode::Dense,
                };
                Self::Union(arrow::Union {
                    union_types: fields
                        .iter()
                        .map(|(_, field)| field.as_ref().try_into())
                        .collect::<Result<Vec<_>, ProtoConvError>>()?,
                    union_mode: union_mode.into(),
                    type_ids: fields.iter().map(|(x, _)| x as i32).collect(),
                })
            }
            DataType::Dictionary(key_type, value_type) => {
                Self::Dictionary(Box::new(arrow::Dictionary {
                    key: Some(Box::new(key_type.as_ref().try_into()?)),
                    value: Some(Box::new(value_type.as_ref().try_into()?)),
                }))
            }
            DataType::Decimal128(precision, scale) => Self::Decimal(arrow::Decimal {
                precision: *precision as u32,
                scale: *scale as i32,
            }),
            DataType::Decimal256(..) => {
                return Err(ProtoConvError::UnsupportedSerialization("Decimal256"))
            }
            DataType::Map(..) => return Err(ProtoConvError::UnsupportedSerialization("Map")),
            DataType::RunEndEncoded(..) => {
                return Err(ProtoConvError::UnsupportedSerialization("RunEndEncoded"))
            }
        };

        Ok(res)
    }
}

impl TryFrom<&arrow::Field> for Field {
    type Error = ProtoConvError;
    fn try_from(field: &arrow::Field) -> Result<Self, Self::Error> {
        let datatype = field.arrow_type.as_deref().required("arrow_type")?;

        Ok(Self::new(field.name.as_str(), datatype, field.nullable))
    }
}

impl TryFrom<&Field> for arrow::Field {
    type Error = ProtoConvError;
    fn try_from(field: &Field) -> Result<Self, Self::Error> {
        let arrow_type = field.data_type().try_into()?;
        Ok(Self {
            name: field.name().to_owned(),
            arrow_type: Some(Box::new(arrow_type)),
            nullable: field.is_nullable(),
            children: Vec::new(),
        })
    }
}

impl From<arrow::TimeUnit> for TimeUnit {
    fn from(time_unit: arrow::TimeUnit) -> Self {
        match time_unit {
            arrow::TimeUnit::Second => TimeUnit::Second,
            arrow::TimeUnit::Millisecond => TimeUnit::Millisecond,
            arrow::TimeUnit::Microsecond => TimeUnit::Microsecond,
            arrow::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

impl From<&TimeUnit> for arrow::TimeUnit {
    fn from(val: &TimeUnit) -> Self {
        match val {
            TimeUnit::Second => arrow::TimeUnit::Second,
            TimeUnit::Millisecond => arrow::TimeUnit::Millisecond,
            TimeUnit::Microsecond => arrow::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => arrow::TimeUnit::Nanosecond,
        }
    }
}

impl From<arrow::IntervalUnit> for IntervalUnit {
    fn from(interval_unit: arrow::IntervalUnit) -> Self {
        match interval_unit {
            arrow::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
            arrow::IntervalUnit::DayTime => IntervalUnit::DayTime,
            arrow::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        }
    }
}

impl From<&IntervalUnit> for arrow::IntervalUnit {
    fn from(interval_unit: &IntervalUnit) -> Self {
        match interval_unit {
            IntervalUnit::YearMonth => arrow::IntervalUnit::YearMonth,
            IntervalUnit::DayTime => arrow::IntervalUnit::DayTime,
            IntervalUnit::MonthDayNano => arrow::IntervalUnit::MonthDayNano,
        }
    }
}

fn parse_i32_to_time_unit(value: &i32) -> Result<TimeUnit, ProtoConvError> {
    arrow::TimeUnit::try_from(*value)
        .map(|t| t.into())
        .map_err(|_| ProtoConvError::UnknownEnumVariant("TimeUnit", *value))
}

fn parse_i32_to_interval_unit(value: &i32) -> Result<IntervalUnit, ProtoConvError> {
    arrow::IntervalUnit::try_from(*value)
        .map(|t| t.into())
        .map_err(|_| ProtoConvError::UnknownEnumVariant("IntervalUnit", *value))
}
