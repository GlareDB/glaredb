use glaredb_core::arrays::datatype::{
    DataType,
    DecimalTypeMeta,
    StructTypeMeta,
    TimeUnit,
    TimestampTypeMeta,
};
use glaredb_core::arrays::field::{ColumnSchema, Field};
use glaredb_error::{DbError, Result, not_implemented};

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
        let fields = match datatype {
            DataType::Struct(m) => m.fields,
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
        let datatype = DataType::Struct(StructTypeMeta::new(children));

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
        basic::Type::BOOLEAN => Ok(DataType::Boolean),
        basic::Type::INT32 => {
            match (
                prim.basic_info.logical_type(),
                prim.basic_info.converted_type(),
            ) {
                (None, basic::ConvertedType::NONE) => Ok(DataType::Int32),
                (None, basic::ConvertedType::UINT_8) => Ok(DataType::UInt8),
                (None, basic::ConvertedType::UINT_16) => Ok(DataType::UInt16),
                (None, basic::ConvertedType::UINT_32) => Ok(DataType::UInt32),
                (None, basic::ConvertedType::INT_8) => Ok(DataType::Int8),
                (None, basic::ConvertedType::INT_16) => Ok(DataType::Int16),
                (None, basic::ConvertedType::INT_32) => Ok(DataType::Int32),
                (None, basic::ConvertedType::DATE) => Ok(DataType::Date32),
                (None, basic::ConvertedType::DECIMAL) => {
                    decimal128_with_prec_scale(prim.precision, prim.scale)
                }
                (None, basic::ConvertedType::TIME_MILLIS) => Ok(DataType::Timestamp(
                    TimestampTypeMeta::new(TimeUnit::Millisecond),
                )),
                (
                    Some(basic::LogicalType::Integer {
                        bit_width,
                        is_signed,
                    }),
                    _,
                ) => match (bit_width, is_signed) {
                    (8, true) => Ok(DataType::Int8),
                    (16, true) => Ok(DataType::Int16),
                    (32, true) => Ok(DataType::Int32),
                    (8, false) => Ok(DataType::UInt8),
                    (16, false) => Ok(DataType::UInt16),
                    (32, false) => Ok(DataType::UInt32),
                    _ => Err(DbError::new(
                        "Cannot convert from INT32 with width {bit_width}",
                    )),
                },
                (Some(basic::LogicalType::Decimal { scale, precision }), _) => {
                    decimal128_with_prec_scale(precision, scale)
                }
                (Some(basic::LogicalType::Timestamp { unit, .. }), _) => match unit {
                    format::TimeUnit::MILLIS(_) => Ok(DataType::Timestamp(TimestampTypeMeta::new(
                        TimeUnit::Millisecond,
                    ))),
                    other => Err(DbError::new(format!(
                        "Unhandled time unit for INT32: {other:?}"
                    ))),
                },
                (Some(basic::LogicalType::Date), _) => Ok(DataType::Date32),
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
                (None, basic::ConvertedType::NONE) => Ok(DataType::Int64),
                (None, basic::ConvertedType::INT_64) => Ok(DataType::Int64),
                (None, basic::ConvertedType::UINT_64) => Ok(DataType::UInt64),
                (None, basic::ConvertedType::DECIMAL) => {
                    decimal128_with_prec_scale(prim.precision, prim.scale)
                }
                (
                    Some(basic::LogicalType::Integer {
                        bit_width,
                        is_signed,
                    }),
                    _,
                ) => match (bit_width, is_signed) {
                    (8, true) => Ok(DataType::Int8),
                    (16, true) => Ok(DataType::Int16),
                    (32, true) => Ok(DataType::Int32),
                    (64, true) => Ok(DataType::Int64),
                    (8, false) => Ok(DataType::UInt8),
                    (16, false) => Ok(DataType::UInt16),
                    (32, false) => Ok(DataType::UInt32),
                    (64, false) => Ok(DataType::UInt64),
                    _ => Err(DbError::new(
                        "Cannot convert from INT32 with width {bit_width}",
                    )),
                },
                (Some(basic::LogicalType::Decimal { scale, precision }), _) => {
                    decimal128_with_prec_scale(precision, scale)
                }
                (Some(basic::LogicalType::Timestamp { unit, .. }), _) => match unit {
                    format::TimeUnit::MILLIS(_) => Ok(DataType::Timestamp(TimestampTypeMeta::new(
                        TimeUnit::Millisecond,
                    ))),
                    format::TimeUnit::MICROS(_) => Ok(DataType::Timestamp(TimestampTypeMeta::new(
                        TimeUnit::Microsecond,
                    ))),
                    format::TimeUnit::NANOS(_) => Ok(DataType::Timestamp(TimestampTypeMeta::new(
                        TimeUnit::Nanosecond,
                    ))),
                },
                (Some(basic::LogicalType::Date), _) => Ok(DataType::Date32),
                (logical, converted) => Err(DbError::new(format!(
                    "Cannot handle INT64 with logical type {logical:?} or converted type {converted:?}",
                ))),
            }
        }
        basic::Type::FLOAT => Ok(DataType::Float32),
        basic::Type::DOUBLE => Ok(DataType::Float64),
        basic::Type::INT96 => Ok(DataType::Timestamp(TimestampTypeMeta::new(
            TimeUnit::Nanosecond,
        ))),
        basic::Type::BYTE_ARRAY => {
            match (
                prim.basic_info.logical_type(),
                prim.basic_info.converted_type(),
            ) {
                (None, basic::ConvertedType::NONE) => Ok(DataType::Binary),
                (None, basic::ConvertedType::UTF8) => Ok(DataType::Utf8),
                (Some(LogicalType::String), _) => Ok(DataType::Utf8),
                (logical, converted) => Err(DbError::new(format!(
                    "Cannot handle BYTE_ARRAY with logical type {logical:?} or converted type {converted:?}",
                ))),
            }
        }
        other => not_implemented!("{other:?}"),
    }
}

fn decimal128_with_prec_scale(prec: i32, scale: i32) -> Result<DataType> {
    let prec: u8 = prec.try_into()?;
    let scale: i8 = scale.try_into()?;

    let meta = DecimalTypeMeta::new(prec, scale);

    // TODO: Validate prec/scale.

    Ok(DataType::Decimal128(meta))
}
