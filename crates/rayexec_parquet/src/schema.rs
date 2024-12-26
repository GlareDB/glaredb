use std::sync::Arc;

use parquet::basic::{
    ConvertedType,
    LogicalType,
    Repetition,
    TimeUnit as ParquetTimeUnit,
    Type as PhysicalType,
};
use parquet::format::{MicroSeconds, MilliSeconds, NanoSeconds};
use parquet::schema::types::{BasicTypeInfo, SchemaDescriptor, Type};
use rayexec_bullet::datatype::{
    DataTypeOld,
    DecimalTypeMeta,
    ListTypeMeta,
    TimeUnit,
    TimestampTypeMeta,
};
use rayexec_bullet::field::{Field, Schema};
use rayexec_bullet::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};

/// Converts a parquet schema to a bullet schema.
///
/// A lot of this logic was taken from the conversion to arrow in the upstream
/// arrow-rs crate.
pub fn from_parquet_schema(parquet_schema: &SchemaDescriptor) -> Result<Schema> {
    match parquet_schema.root_schema() {
        Type::GroupType { fields, .. } => {
            let fields = convert_types_to_fields(fields)?;
            Ok(Schema::new(fields))
        }
        Type::PrimitiveType { .. } => unreachable!("schema type is not primitive"),
    }
}

pub fn to_parquet_schema(schema: &Schema) -> Result<SchemaDescriptor> {
    let types = schema
        .fields
        .iter()
        .map(|f| Ok(Arc::new(to_parquet_type(f)?)))
        .collect::<Result<Vec<_>>>()?;

    Ok(SchemaDescriptor::new(Arc::new(
        Type::group_type_builder("schema")
            .with_fields(types)
            .build()
            .context("failed to build root group type")?,
    )))
}

fn to_parquet_type(field: &Field) -> Result<Type> {
    let rep = if field.nullable {
        // TODO: CHANGE ME.
        Repetition::REQUIRED
    } else {
        Repetition::REQUIRED
    };

    let result = match &field.datatype {
        DataTypeOld::Boolean => Type::primitive_type_builder(&field.name, PhysicalType::BOOLEAN)
            .with_repetition(rep)
            .build(),
        DataTypeOld::Int8 => Type::primitive_type_builder(&field.name, PhysicalType::INT32)
            .with_repetition(rep)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            }))
            .build(),
        DataTypeOld::Int16 => Type::primitive_type_builder(&field.name, PhysicalType::INT32)
            .with_repetition(rep)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: true,
            }))
            .build(),
        DataTypeOld::Int32 => Type::primitive_type_builder(&field.name, PhysicalType::INT32)
            .with_repetition(rep)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: true,
            }))
            .build(),
        DataTypeOld::Int64 => Type::primitive_type_builder(&field.name, PhysicalType::INT64)
            .with_repetition(rep)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 64,
                is_signed: true,
            }))
            .build(),
        DataTypeOld::UInt8 => Type::primitive_type_builder(&field.name, PhysicalType::INT32)
            .with_repetition(rep)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: false,
            }))
            .build(),
        DataTypeOld::UInt16 => Type::primitive_type_builder(&field.name, PhysicalType::INT32)
            .with_repetition(rep)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: false,
            }))
            .build(),
        DataTypeOld::UInt32 => Type::primitive_type_builder(&field.name, PhysicalType::INT32)
            .with_repetition(rep)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: false,
            }))
            .build(),
        DataTypeOld::UInt64 => Type::primitive_type_builder(&field.name, PhysicalType::INT64)
            .with_repetition(rep)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 64,
                is_signed: false,
            }))
            .build(),
        DataTypeOld::Float32 => Type::primitive_type_builder(&field.name, PhysicalType::FLOAT)
            .with_repetition(rep)
            .build(),
        DataTypeOld::Float64 => Type::primitive_type_builder(&field.name, PhysicalType::DOUBLE)
            .with_repetition(rep)
            .build(),
        DataTypeOld::Timestamp(meta) => {
            let logical_type = match meta.unit {
                TimeUnit::Second => None,
                TimeUnit::Millisecond => Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: false,
                    unit: ParquetTimeUnit::MILLIS(MilliSeconds::new()),
                }),
                TimeUnit::Microsecond => Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: false,
                    unit: ParquetTimeUnit::MICROS(MicroSeconds::new()),
                }),
                TimeUnit::Nanosecond => Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: false,
                    unit: ParquetTimeUnit::NANOS(NanoSeconds::new()),
                }),
            };

            Type::primitive_type_builder(&field.name, PhysicalType::INT64)
                .with_repetition(rep)
                .with_logical_type(logical_type)
                .build()
        }
        DataTypeOld::Utf8 => Type::primitive_type_builder(&field.name, PhysicalType::BYTE_ARRAY)
            .with_repetition(rep)
            .with_logical_type(Some(LogicalType::String))
            .build(),
        other => {
            return Err(RayexecError::new(format!(
                "Unimplemented type conversion to parquet type: {other}"
            )))
        }
    };

    result.context("failed to build parquet type")
}

fn convert_types_to_fields<T: AsRef<Type>>(typs: &[T]) -> Result<Vec<Field>> {
    let mut fields = Vec::with_capacity(typs.len());

    for parquet_type in typs.iter() {
        let field = convert_type_to_field(parquet_type)?;
        fields.push(field);
    }

    Ok(fields)
}

fn convert_type_to_field(parquet_type: impl AsRef<Type>) -> Result<Field> {
    let parquet_type = parquet_type.as_ref();
    let dt = if parquet_type.is_primitive() {
        convert_primitive(parquet_type)?
    } else {
        convert_complex(parquet_type)?
    };

    let field = Field::new(parquet_type.name(), dt, true); // TODO: Nullable from repetition.

    Ok(field)
}

fn convert_complex(parquet_type: &Type) -> Result<DataTypeOld> {
    match parquet_type {
        Type::GroupType { basic_info, fields } => {
            match basic_info.converted_type() {
                ConvertedType::LIST => {
                    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
                    if fields.len() != 1 {
                        return Err(RayexecError::new(format!(
                            "List can only have a single type, got {}",
                            fields.len(),
                        )));
                    }

                    let nested = &fields[0];
                    if !nested.get_basic_info().has_repetition() {
                        return Err(RayexecError::new("List field requires repetition"));
                    }
                    if nested.get_basic_info().repetition() != Repetition::REPEATED {
                        return Err(RayexecError::new("List field requires REPEATED repetition"));
                    }

                    // // List<Integer> (nullable list, non-null elements)
                    // optional group my_list (LIST) {
                    //   repeated int32 element;
                    // }
                    if nested.is_primitive() {
                        let nested_type = convert_primitive(nested.as_ref())?;
                        return Ok(DataTypeOld::List(ListTypeMeta::new(nested_type)));
                    }

                    // TODO: Other backwards compat types.

                    let inner_fields = nested.get_fields();
                    if inner_fields.len() != 1 {
                        return Err(RayexecError::new(format!(
                            "Unsupported number of inner fields for list, expected 1, got {}",
                            inner_fields.len(),
                        )));
                    }

                    let inner_field = &inner_fields[0];
                    let inner_field = convert_type_to_field(inner_field)?;

                    Ok(DataTypeOld::List(ListTypeMeta::new(inner_field.datatype)))
                }
                ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE => {
                    not_implemented!("parquet map")
                }
                _ => {
                    // let struct_fields = convert_group_fields(parquet_type)?;
                    not_implemented!("parquet struct")
                }
            }
        }
        Type::PrimitiveType { .. } => unreachable!(),
    }
}

/// Convert a primitive type to a bullet data type.
///
/// <https://github.com/apache/parquet-format/blob/master/LogicalTypes.md>
fn convert_primitive(parquet_type: &Type) -> Result<DataTypeOld> {
    match parquet_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            type_length: _,
            scale,
            precision,
        } => match physical_type {
            parquet::basic::Type::BOOLEAN => Ok(DataTypeOld::Boolean),
            parquet::basic::Type::INT32 => from_int32(basic_info, *scale, *precision),
            parquet::basic::Type::INT64 => from_int64(basic_info, *scale, *precision),
            parquet::basic::Type::INT96 => Ok(DataTypeOld::Timestamp(TimestampTypeMeta::new(
                TimeUnit::Nanosecond,
            ))),
            parquet::basic::Type::FLOAT => Ok(DataTypeOld::Float32),
            parquet::basic::Type::DOUBLE => Ok(DataTypeOld::Float64),
            parquet::basic::Type::BYTE_ARRAY => from_byte_array(basic_info, *precision, *scale),
            parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => {
                not_implemented!("parquet fixed len byte array")
            }
        },
        Type::GroupType { .. } => unreachable!(),
    }
}

fn decimal_type(precision: i32, scale: i32) -> Result<DataTypeOld> {
    if precision < 0 {
        return Err(RayexecError::new("Precision cannot be negative"));
    }
    if scale > precision {
        return Err(RayexecError::new("Scale cannot be greater than precision"));
    }

    if precision <= Decimal64Type::MAX_PRECISION as i32 {
        Ok(DataTypeOld::Decimal64(DecimalTypeMeta::new(
            precision as u8,
            scale as i8,
        )))
    } else if precision <= Decimal128Type::MAX_PRECISION as i32 {
        Ok(DataTypeOld::Decimal128(DecimalTypeMeta::new(
            precision as u8,
            scale as i8,
        )))
    } else {
        Err(RayexecError::new(format!(
            "Decimal precision of {precision} is to high"
        )))
    }
}

fn from_int32(info: &BasicTypeInfo, _scale: i32, _precision: i32) -> Result<DataTypeOld> {
    match (info.logical_type(), info.converted_type()) {
        (None, ConvertedType::NONE) => Ok(DataTypeOld::Int32),
        (
            Some(
                ref t @ LogicalType::Integer {
                    bit_width,
                    is_signed,
                },
            ),
            _,
        ) => match (bit_width, is_signed) {
            (8, true) => Ok(DataTypeOld::Int8),
            (16, true) => Ok(DataTypeOld::Int16),
            (32, true) => Ok(DataTypeOld::Int32),
            (8, false) => Ok(DataTypeOld::UInt8),
            (16, false) => Ok(DataTypeOld::UInt16),
            (32, false) => Ok(DataTypeOld::UInt32),
            _ => Err(RayexecError::new(format!(
                "Cannot create INT32 physical type from {:?}",
                t
            ))),
        },
        (Some(LogicalType::Decimal { scale, precision }), _) => {
            Ok(DataTypeOld::Decimal128(DecimalTypeMeta {
                precision: precision as u8,
                scale: scale as i8,
            }))
        }
        (Some(LogicalType::Date), _) => unimplemented!(),
        (Some(LogicalType::Time { unit, .. }), _) => match unit {
            ParquetTimeUnit::MILLIS(_) => unimplemented!(),
            _ => Err(RayexecError::new(format!(
                "Cannot create INT32 physical type from {:?}",
                unit
            ))),
        },
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#unknown-always-null
        (Some(LogicalType::Unknown), _) => Ok(DataTypeOld::Null),
        (None, ConvertedType::UINT_8) => Ok(DataTypeOld::UInt8),
        (None, ConvertedType::UINT_16) => Ok(DataTypeOld::UInt16),
        (None, ConvertedType::UINT_32) => Ok(DataTypeOld::UInt32),
        (None, ConvertedType::INT_8) => Ok(DataTypeOld::Int8),
        (None, ConvertedType::INT_16) => Ok(DataTypeOld::Int16),
        (None, ConvertedType::INT_32) => Ok(DataTypeOld::Int32),
        (None, ConvertedType::DATE) => Ok(DataTypeOld::Date32),
        (None, ConvertedType::TIME_MILLIS) => unimplemented!(),
        (None, ConvertedType::DECIMAL) => unimplemented!(),
        (logical, converted) => Err(RayexecError::new(format!(
            "Unable to convert parquet INT32 logical type {:?} or converted type {}",
            logical, converted
        ))),
    }
}

fn from_int64(info: &BasicTypeInfo, _scale: i32, _precision: i32) -> Result<DataTypeOld> {
    match (info.logical_type(), info.converted_type()) {
        (None, ConvertedType::NONE) => Ok(DataTypeOld::Int64),
        (
            Some(LogicalType::Integer {
                bit_width: 64,
                is_signed,
            }),
            _,
        ) => match is_signed {
            true => Ok(DataTypeOld::Int64),
            false => Ok(DataTypeOld::UInt64),
        },
        (Some(LogicalType::Time { unit, .. }), _) => match unit {
            ParquetTimeUnit::MILLIS(_) => Err(RayexecError::new(
                "Cannot create INT64 from MILLIS time unit",
            )),
            ParquetTimeUnit::MICROS(_) => unimplemented!(),
            ParquetTimeUnit::NANOS(_) => unimplemented!(),
        },
        (
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            }),
            _,
        ) => {
            let unit = match unit {
                ParquetTimeUnit::MILLIS(_) => TimeUnit::Millisecond,
                ParquetTimeUnit::MICROS(_) => TimeUnit::Microsecond,
                ParquetTimeUnit::NANOS(_) => TimeUnit::Nanosecond,
            };
            if is_adjusted_to_u_t_c {
                not_implemented!("parquet timestamp adjusted to utc");
            }
            Ok(DataTypeOld::Timestamp(TimestampTypeMeta::new(unit)))
        }
        (None, ConvertedType::INT_64) => Ok(DataTypeOld::Int64),
        (None, ConvertedType::UINT_64) => Ok(DataTypeOld::UInt64),
        (None, ConvertedType::TIME_MICROS) => unimplemented!(),
        (None, ConvertedType::TIMESTAMP_MILLIS) => unimplemented!(),
        (None, ConvertedType::TIMESTAMP_MICROS) => unimplemented!(),
        (Some(LogicalType::Decimal { scale, precision }), _) => decimal_type(precision, scale),
        (None, ConvertedType::DECIMAL) => unimplemented!(),
        (logical, converted) => Err(RayexecError::new(format!(
            "Unable to convert parquet INT64 logical type {:?} or converted type {}",
            logical, converted
        ))),
    }
}

fn from_byte_array(info: &BasicTypeInfo, _precision: i32, _scale: i32) -> Result<DataTypeOld> {
    match (info.logical_type(), info.converted_type()) {
        (Some(LogicalType::String), _) => Ok(DataTypeOld::Utf8),
        (Some(LogicalType::Json), _) => Ok(DataTypeOld::Utf8),
        (Some(LogicalType::Bson), _) => Ok(DataTypeOld::Binary),
        (Some(LogicalType::Enum), _) => Ok(DataTypeOld::Binary),
        (None, ConvertedType::NONE) => Ok(DataTypeOld::Binary),
        (None, ConvertedType::JSON) => Ok(DataTypeOld::Utf8),
        (None, ConvertedType::BSON) => Ok(DataTypeOld::Binary),
        (None, ConvertedType::ENUM) => Ok(DataTypeOld::Binary),
        (None, ConvertedType::UTF8) => Ok(DataTypeOld::Utf8),
        (
            Some(LogicalType::Decimal {
                scale: _,
                precision: _,
            }),
            _,
        ) => unimplemented!(),
        (None, ConvertedType::DECIMAL) => unimplemented!(),
        (logical, converted) => Err(RayexecError::new(format!(
            "Unable to convert parquet BYTE_ARRAY logical type {:?} or converted type {}",
            logical, converted
        ))),
    }
}
