use glaredb_core::arrays::datatype::{
    DataType,
    DataTypeId,
    DecimalTypeMeta,
    TimeUnit,
    TimestampTypeMeta,
};
use glaredb_core::arrays::field::{ColumnSchema, Field};
use glaredb_error::{DbError, Result, ResultExt, not_implemented};

use super::types::{GroupType, PrimitiveType, SchemaDescriptor, SchemaType};
use super::visitor::TypeVisitor;
use crate::basic::LogicalType;
use crate::{basic, format};

#[derive(Debug)]
pub struct ColumnSchemaTypeVisitor;

impl ColumnSchemaTypeVisitor {
    /// Convert a parquet schema descriptor to a column schema.
    pub fn convert_schema(&mut self, desc: &SchemaDescriptor) -> Result<ColumnSchema> {
        let datatype = self.visit_struct(&desc.schema, ())?;
        let fields = match datatype.id() {
            DataTypeId::Struct => datatype.try_get_struct_type_meta()?.fields.clone(),
            other => panic!("unexpected type: {other:?}"),
        };

        Ok(ColumnSchema::new(fields))
    }
}

impl TypeVisitor<DataType, ()> for ColumnSchemaTypeVisitor {
    fn visit_primitive(
        &mut self,
        primitive_type: &PrimitiveType,
        _context: (),
    ) -> Result<DataType> {
        convert_primitive(primitive_type)
    }

    fn visit_struct(&mut self, struct_type: &GroupType, _context: ()) -> Result<DataType> {
        let mut children = Vec::with_capacity(struct_type.fields.len());

        for field in &struct_type.fields {
            let converted = self.dispatch(field, ())?;
            let field = Field::new(field.name(), converted, true);
            children.push(field);
        }
        let datatype = DataType::struct_type(children);

        Ok(datatype)
    }

    fn visit_map(&mut self, _map_type: &GroupType, _context: ()) -> Result<DataType> {
        not_implemented!("Visit MAP")
    }

    fn visit_list_with_item(
        &mut self,
        _list_type: &GroupType,
        _item_type: &SchemaType,
        _context: (),
    ) -> Result<DataType> {
        not_implemented!("Visit LIST with item")
    }
}

/// Convert a primtive type to a datatype.
fn convert_primitive(prim: &PrimitiveType) -> Result<DataType> {
    match prim.physical_type {
        basic::Type::BOOLEAN => Ok(DataType::boolean()),
        basic::Type::INT32 => {
            match (
                prim.basic_info.logical_type(),
                prim.basic_info.converted_type(),
            ) {
                (None, basic::ConvertedType::NONE) => Ok(DataType::int32()),
                (None, basic::ConvertedType::UINT_8) => Ok(DataType::uint8()),
                (None, basic::ConvertedType::UINT_16) => Ok(DataType::uint16()),
                (None, basic::ConvertedType::UINT_32) => Ok(DataType::uint32()),
                (None, basic::ConvertedType::INT_8) => Ok(DataType::int8()),
                (None, basic::ConvertedType::INT_16) => Ok(DataType::int16()),
                (None, basic::ConvertedType::INT_32) => Ok(DataType::int32()),
                (None, basic::ConvertedType::DATE) => Ok(DataType::date32()),
                (None, basic::ConvertedType::DECIMAL) => {
                    let meta = decimal_type_meta(prim.precision, prim.scale)?;
                    Ok(DataType::decimal64(meta))
                }
                (None, basic::ConvertedType::TIME_MILLIS) => Ok(DataType::timestamp(
                    TimestampTypeMeta::new(TimeUnit::Millisecond),
                )),
                (
                    Some(basic::LogicalType::Integer {
                        bit_width,
                        is_signed,
                    }),
                    _,
                ) => match (bit_width, is_signed) {
                    (8, true) => Ok(DataType::int8()),
                    (16, true) => Ok(DataType::int16()),
                    (32, true) => Ok(DataType::int32()),
                    (8, false) => Ok(DataType::uint8()),
                    (16, false) => Ok(DataType::uint16()),
                    (32, false) => Ok(DataType::uint32()),
                    _ => Err(DbError::new(
                        "Cannot convert from INT32 with width {bit_width}",
                    )),
                },
                (Some(basic::LogicalType::Decimal { scale, precision }), _) => {
                    let meta = decimal_type_meta(precision, scale)?;
                    Ok(DataType::decimal64(meta))
                }
                // (Some(basic::LogicalType::Timestamp { unit, .. }), _) => match unit {
                //     format::TimeUnit::MILLIS(_) => Ok(DataType::Timestamp(TimestampTypeMeta::new(
                //         TimeUnit::Millisecond,
                //     ))),
                //     other => Err(DbError::new(format!(
                //         "Unhandled time unit for INT32: {other:?}"
                //     ))),
                // },
                (Some(basic::LogicalType::Date), _) => Ok(DataType::date32()),
                (logical, converted) => Err(DbError::new(format!(
                    "Cannot handle INT32 with logical type {logical:?} or converted type {converted:?}",
                ))),
            }
        }
        basic::Type::INT64 => {
            match (
                prim.basic_info.logical_type(),
                prim.basic_info.converted_type(),
            ) {
                (None, basic::ConvertedType::NONE) => Ok(DataType::int64()),
                (None, basic::ConvertedType::INT_64) => Ok(DataType::int64()),
                (None, basic::ConvertedType::UINT_64) => Ok(DataType::uint64()),
                (None, basic::ConvertedType::DECIMAL) => {
                    let meta = decimal_type_meta(prim.precision, prim.scale)?;
                    Ok(DataType::decimal64(meta))
                }
                (
                    Some(basic::LogicalType::Integer {
                        bit_width,
                        is_signed,
                    }),
                    _,
                ) => match (bit_width, is_signed) {
                    (8, true) => Ok(DataType::int8()),
                    (16, true) => Ok(DataType::int16()),
                    (32, true) => Ok(DataType::int32()),
                    (64, true) => Ok(DataType::int64()),
                    (8, false) => Ok(DataType::uint8()),
                    (16, false) => Ok(DataType::uint16()),
                    (32, false) => Ok(DataType::uint32()),
                    (64, false) => Ok(DataType::uint64()),
                    _ => Err(DbError::new(
                        "Cannot convert from INT32 with width {bit_width}",
                    )),
                },
                (Some(basic::LogicalType::Decimal { scale, precision }), _) => {
                    let meta = decimal_type_meta(precision, scale)?;
                    Ok(DataType::decimal64(meta))
                }
                (Some(basic::LogicalType::Timestamp { unit, .. }), _) => match unit {
                    format::TimeUnit::MILLIS(_) => Ok(DataType::timestamp(TimestampTypeMeta::new(
                        TimeUnit::Millisecond,
                    ))),
                    format::TimeUnit::MICROS(_) => Ok(DataType::timestamp(TimestampTypeMeta::new(
                        TimeUnit::Microsecond,
                    ))),
                    format::TimeUnit::NANOS(_) => Ok(DataType::timestamp(TimestampTypeMeta::new(
                        TimeUnit::Nanosecond,
                    ))),
                },
                (Some(basic::LogicalType::Date), _) => Ok(DataType::date64()),
                (logical, converted) => Err(DbError::new(format!(
                    "Cannot handle INT64 with logical type {logical:?} or converted type {converted:?}",
                ))),
            }
        }
        basic::Type::FLOAT => Ok(DataType::float32()),
        basic::Type::DOUBLE => Ok(DataType::float64()),
        basic::Type::INT96 => Ok(DataType::timestamp(TimestampTypeMeta::new(
            TimeUnit::Nanosecond,
        ))),
        basic::Type::FIXED_LEN_BYTE_ARRAY => {
            match (
                prim.basic_info.logical_type(),
                prim.basic_info.converted_type(),
            ) {
                (Some(LogicalType::Float16), _) => Ok(DataType::float16()),
                (logical, converted) => Err(DbError::new(format!(
                    "Cannot handle FIXED_LEN_BYTE_ARRAY with logical type {logical:?} or converted type {converted:?}",
                ))),
            }
        }
        basic::Type::BYTE_ARRAY => {
            match (
                prim.basic_info.logical_type(),
                prim.basic_info.converted_type(),
            ) {
                (None, basic::ConvertedType::NONE) => Ok(DataType::binary()),
                (None, basic::ConvertedType::UTF8) => Ok(DataType::utf8()),
                (Some(LogicalType::String), _) => Ok(DataType::utf8()),
                (logical, converted) => Err(DbError::new(format!(
                    "Cannot handle BYTE_ARRAY with logical type {logical:?} or converted type {converted:?}",
                ))),
            }
        }
    }
}

fn decimal_type_meta(prec: i32, scale: i32) -> Result<DecimalTypeMeta> {
    if prec < 0 {
        return Err(DbError::new("Decimal precision cannot be negative"));
    }
    let prec: u8 = prec.try_into().context("Decimal precision too large")?;
    let scale: i8 = scale.try_into().context("Decimal scale too large")?;

    Ok(DecimalTypeMeta::new(prec, scale))
}
