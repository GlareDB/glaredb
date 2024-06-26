use crate::{
    array::{
        Array, ArrayAccessor, BooleanArray, BooleanValuesBuffer, Decimal128Array, Decimal64Array,
        DecimalArray, OffsetIndex, PrimitiveArray, ValuesBuffer, VarlenArray, VarlenValuesBuffer,
    },
    datatype::DataType,
    executor::scalar::UnaryExecutor,
    scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType},
};
use num::{NumCast, PrimInt, ToPrimitive};
use rayexec_error::{RayexecError, Result};
use std::{
    fmt::{self, Display},
    ops::{Div, Mul},
};

use super::{
    format::{
        BoolFormatter, Decimal128Formatter, Decimal64Formatter, Float32Formatter, Float64Formatter,
        Formatter, Int16Formatter, Int32Formatter, Int64Formatter, Int8Formatter,
        TimestampMicrosecondsFormatter, TimestampMillisecondsFormatter,
        TimestampNanosecondsFormatter, TimestampSecondsFormatter, UInt16Formatter, UInt32Formatter,
        UInt64Formatter, UInt8Formatter,
    },
    parse::{
        BoolParser, Date32Parser, Decimal128Parser, Decimal64Parser, Float32Parser, Float64Parser,
        Int16Parser, Int32Parser, Int64Parser, Int8Parser, IntervalParser, Parser, UInt16Parser,
        UInt32Parser, UInt64Parser, UInt8Parser,
    },
};

/// Cast an array to some other data type.
pub fn cast_array(arr: &Array, to: &DataType) -> Result<Array> {
    Ok(match (arr, to) {
        // Primitive numeric casts
        // From UInt8
        (Array::UInt8(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::UInt8) => Array::UInt8(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::UInt16) => Array::UInt16(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::UInt32) => Array::UInt32(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::UInt64) => Array::UInt64(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::Decimal64(meta)) => Array::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array::UInt8(arr), DataType::Decimal128(meta)) => Array::Decimal128(Decimal128Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
        )),
        // From UInt16
        (Array::UInt16(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::UInt8) => Array::UInt8(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::UInt16) => Array::UInt16(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::UInt32) => Array::UInt32(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::UInt64) => Array::UInt64(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::Decimal64(meta)) => Array::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array::UInt16(arr), DataType::Decimal128(meta)) => {
            Array::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                cast_int_to_decimal::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
            ))
        }

        // From UInt32
        (Array::UInt32(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::UInt8) => Array::UInt8(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::UInt16) => Array::UInt16(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::UInt32) => Array::UInt32(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::UInt64) => Array::UInt64(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::Decimal64(meta)) => Array::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array::UInt32(arr), DataType::Decimal128(meta)) => {
            Array::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                cast_int_to_decimal::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
            ))
        }

        // From UInt64
        (Array::UInt64(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::UInt8) => Array::UInt8(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::UInt16) => Array::UInt16(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::UInt32) => Array::UInt32(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::UInt64) => Array::UInt64(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::Decimal64(meta)) => Array::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array::UInt64(arr), DataType::Decimal128(meta)) => {
            Array::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                cast_int_to_decimal::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
            ))
        }

        // From Int8
        (Array::Int8(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::UInt8) => Array::UInt8(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::UInt16) => Array::UInt16(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::UInt32) => Array::UInt32(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::UInt64) => Array::UInt64(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::Decimal64(meta)) => Array::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array::Int8(arr), DataType::Decimal128(meta)) => Array::Decimal128(Decimal128Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
        )),

        // From Int16
        (Array::Int16(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::UInt8) => Array::UInt8(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::UInt16) => Array::UInt16(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::UInt32) => Array::UInt32(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::UInt64) => Array::UInt64(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::Decimal64(meta)) => Array::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array::Int16(arr), DataType::Decimal128(meta)) => Array::Decimal128(Decimal128Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
        )),

        // From Int32
        (Array::Int32(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::UInt8) => Array::UInt8(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::UInt16) => Array::UInt16(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::UInt32) => Array::UInt32(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::UInt64) => Array::UInt64(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::Decimal64(meta)) => Array::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array::Int32(arr), DataType::Decimal128(meta)) => Array::Decimal128(Decimal128Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
        )),

        // From Int64
        (Array::Int64(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::UInt8) => Array::UInt8(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::UInt16) => Array::UInt16(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::UInt32) => Array::UInt32(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::UInt64) => Array::UInt64(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::Decimal64(meta)) => Array::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array::Int64(arr), DataType::Decimal128(meta)) => Array::Decimal128(Decimal128Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
        )),

        // From Float32
        (Array::Float32(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::Float32(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::Float32(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::Float32(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::Float32(arr), DataType::UInt8) => Array::UInt8(cast_primitive_numeric(arr)?),
        (Array::Float32(arr), DataType::UInt16) => Array::UInt16(cast_primitive_numeric(arr)?),
        (Array::Float32(arr), DataType::UInt32) => Array::UInt32(cast_primitive_numeric(arr)?),
        (Array::Float32(arr), DataType::UInt64) => Array::UInt64(cast_primitive_numeric(arr)?),
        (Array::Float32(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::Float32(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        // From FLoat64
        (Array::Float64(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::Float64(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::Float64(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::Float64(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::Float64(arr), DataType::UInt8) => Array::UInt8(cast_primitive_numeric(arr)?),
        (Array::Float64(arr), DataType::UInt16) => Array::UInt16(cast_primitive_numeric(arr)?),
        (Array::Float64(arr), DataType::UInt32) => Array::UInt32(cast_primitive_numeric(arr)?),
        (Array::Float64(arr), DataType::UInt64) => Array::UInt64(cast_primitive_numeric(arr)?),
        (Array::Float64(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::Float64(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),

        // From Utf8
        (Array::Utf8(arr), datatype) => cast_from_utf8_array(arr, datatype)?,
        (Array::LargeUtf8(arr), datatype) => cast_from_utf8_array(arr, datatype)?,

        // To Utf8
        (arr, DataType::Utf8) => Array::Utf8(cast_to_utf8_array(arr)?),
        (arr, DataType::LargeUtf8) => Array::Utf8(cast_to_utf8_array(arr)?),

        (arr, to) => {
            return Err(RayexecError::new(format!(
                "Unable to cast from {} to {to}",
                arr.datatype(),
            )))
        }
    })
}

pub fn cast_from_utf8_array<O>(arr: &VarlenArray<str, O>, datatype: &DataType) -> Result<Array>
where
    O: OffsetIndex,
{
    Ok(match datatype {
        DataType::Boolean => Array::Boolean(cast_parse_boolean(arr)?),
        DataType::Int8 => Array::Int8(cast_parse_primitive(arr, Int8Parser::default())?),
        DataType::Int16 => Array::Int16(cast_parse_primitive(arr, Int16Parser::default())?),
        DataType::Int32 => Array::Int32(cast_parse_primitive(arr, Int32Parser::default())?),
        DataType::Int64 => Array::Int64(cast_parse_primitive(arr, Int64Parser::default())?),
        DataType::UInt8 => Array::UInt8(cast_parse_primitive(arr, UInt8Parser::default())?),
        DataType::UInt16 => Array::UInt16(cast_parse_primitive(arr, UInt16Parser::default())?),
        DataType::UInt32 => Array::UInt32(cast_parse_primitive(arr, UInt32Parser::default())?),
        DataType::UInt64 => Array::UInt64(cast_parse_primitive(arr, UInt64Parser::default())?),
        DataType::Float32 => Array::Float32(cast_parse_primitive(arr, Float32Parser::default())?),
        DataType::Float64 => Array::Float64(cast_parse_primitive(arr, Float64Parser::default())?),
        DataType::Decimal64(meta) => {
            let primitive =
                cast_parse_primitive(arr, Decimal64Parser::new(meta.precision, meta.scale))?;
            Array::Decimal64(DecimalArray::new(meta.precision, meta.scale, primitive))
        }
        DataType::Decimal128(meta) => {
            let primitive =
                cast_parse_primitive(arr, Decimal128Parser::new(meta.precision, meta.scale))?;
            Array::Decimal128(DecimalArray::new(meta.precision, meta.scale, primitive))
        }
        DataType::Date32 => Array::Date32(cast_parse_primitive(arr, Date32Parser)?),
        DataType::Interval => {
            Array::Interval(cast_parse_primitive(arr, IntervalParser::default())?)
        }
        other => {
            return Err(RayexecError::new(format!(
                "Unable to cast utf8 array to {other}"
            )))
        }
    })
}

pub fn cast_to_utf8_array<O>(arr: &Array) -> Result<VarlenArray<str, O>>
where
    O: OffsetIndex,
{
    Ok(match arr {
        Array::Boolean(arr) => format_values_into_varlen(arr, BoolFormatter::default())?,
        Array::Int8(arr) => format_values_into_varlen(arr, Int8Formatter::default())?,
        Array::Int16(arr) => format_values_into_varlen(arr, Int16Formatter::default())?,
        Array::Int32(arr) => format_values_into_varlen(arr, Int32Formatter::default())?,
        Array::Int64(arr) => format_values_into_varlen(arr, Int64Formatter::default())?,
        Array::UInt8(arr) => format_values_into_varlen(arr, UInt8Formatter::default())?,
        Array::UInt16(arr) => format_values_into_varlen(arr, UInt16Formatter::default())?,
        Array::UInt32(arr) => format_values_into_varlen(arr, UInt32Formatter::default())?,
        Array::UInt64(arr) => format_values_into_varlen(arr, UInt64Formatter::default())?,
        Array::Float32(arr) => format_values_into_varlen(arr, Float32Formatter::default())?,
        Array::Float64(arr) => format_values_into_varlen(arr, Float64Formatter::default())?,
        Array::Decimal64(arr) => format_values_into_varlen(
            arr.get_primitive(),
            Decimal64Formatter::new(arr.precision(), arr.scale()),
        )?,
        Array::Decimal128(arr) => format_values_into_varlen(
            arr.get_primitive(),
            Decimal128Formatter::new(arr.precision(), arr.scale()),
        )?,
        Array::TimestampSeconds(arr) => {
            format_values_into_varlen(arr, TimestampSecondsFormatter::default())?
        }
        Array::TimestampMilliseconds(arr) => {
            format_values_into_varlen(arr, TimestampMillisecondsFormatter::default())?
        }
        Array::TimestampMicroseconds(arr) => {
            format_values_into_varlen(arr, TimestampMicrosecondsFormatter::default())?
        }
        Array::TimestampNanoseconds(arr) => {
            format_values_into_varlen(arr, TimestampNanosecondsFormatter::default())?
        }
        _ => unimplemented!(),
    })
}

/// Helper for taking an arbitrary array and producing a varlen array with the
/// formatted values.
fn format_values_into_varlen<O, F, T, A, I>(
    array: A,
    mut formatter: F,
) -> Result<VarlenArray<str, O>>
where
    T: Display,
    O: OffsetIndex,
    A: ArrayAccessor<T, ValueIter = I>,
    I: Iterator<Item = T>,
    F: Formatter<Type = T>,
{
    let mut buffer = VarlenValuesBuffer::default();
    let mut string_buf = String::new();

    match array.validity() {
        Some(validity) => {
            for (value, valid) in array.values_iter().zip(validity.iter()) {
                if valid {
                    string_buf.clear();
                    formatter
                        .write(&value, &mut string_buf)
                        .map_err(|_| RayexecError::new(format!("Failed to format {value}")))?;
                    buffer.push_value(string_buf.as_str());
                } else {
                    buffer.push_value("");
                }
            }
        }
        None => {
            for value in array.values_iter() {
                string_buf.clear();
                formatter
                    .write(&value, &mut string_buf)
                    .map_err(|_| RayexecError::new(format!("Failed to format {value}")))?;
                buffer.push_value(string_buf.as_str());
            }
        }
    }

    let out = VarlenArray::new(buffer, array.validity().cloned());

    Ok(out)
}

/// Cast from a utf8 array to a primitive array by parsing the utf8 values.
fn cast_parse_primitive<O, T, P>(
    arr: &VarlenArray<str, O>,
    mut parser: P,
) -> Result<PrimitiveArray<T>>
where
    T: Default + Display,
    O: OffsetIndex,
    P: Parser<Type = T>,
{
    let mut new_values = Vec::with_capacity(arr.len());
    let operation = |val| {
        parser
            .parse(val)
            .ok_or_else(|| RayexecError::new(format!("Failed to parse '{val}'")))
    };
    UnaryExecutor::try_execute(arr, operation, &mut new_values)?;

    Ok(PrimitiveArray::new(new_values, arr.validity().cloned()))
}

fn cast_parse_boolean<O>(arr: &VarlenArray<str, O>) -> Result<BooleanArray>
where
    O: OffsetIndex,
{
    let mut buf = BooleanValuesBuffer::with_capacity(arr.len());
    let operation = |val| {
        BoolParser
            .parse(val)
            .ok_or_else(|| RayexecError::new(format!("Failed to parse '{val}'")))
    };
    UnaryExecutor::try_execute(arr, operation, &mut buf)?;

    Ok(BooleanArray::new(buf, arr.validity().cloned()))
}

/// Fallibly cast from primitive type A to primitive type B.
fn cast_primitive_numeric<A, B>(arr: &PrimitiveArray<A>) -> Result<PrimitiveArray<B>>
where
    A: Copy + ToPrimitive + fmt::Display,
    B: NumCast,
{
    let mut new_vals = Vec::with_capacity(arr.len());
    for val in arr.values().as_ref().iter() {
        new_vals
            .push(B::from(*val).ok_or_else(|| RayexecError::new(format!("Failed to cast {val}")))?);
    }

    Ok(match arr.validity() {
        Some(validity) => PrimitiveArray::new(new_vals, Some(validity.clone())),
        None => PrimitiveArray::from(new_vals),
    })
}

/// Cast a primitive int type to the primitive representation of a decimal.
fn cast_int_to_decimal<I, D>(
    arr: &PrimitiveArray<I>,
    precision: u8,
    scale: i8,
) -> Result<PrimitiveArray<D::Primitive>>
where
    I: PrimInt + fmt::Display,
    D: DecimalType,
{
    let mut new_vals: Vec<D::Primitive> = Vec::with_capacity(arr.len());

    // Convert everything to the primitive.
    for val in arr.values().as_ref().iter() {
        new_vals.push(
            <D::Primitive as NumCast>::from(*val)
                .ok_or_else(|| RayexecError::new(format!("Failed to cast {val}")))?,
        );
    }

    // Scale everything.
    let scale_amount = <D::Primitive as NumCast>::from(10.pow(scale.unsigned_abs() as u32))
        .expect("to be in range");
    if scale > 0 {
        new_vals.iter_mut().for_each(|v| *v = v.mul(scale_amount))
    } else {
        new_vals.iter_mut().for_each(|v| *v = v.div(scale_amount))
    }

    // Validate precision.
    // TODO: Skip nulls
    for v in &new_vals {
        D::validate_precision(*v, precision)?;
    }

    Ok(PrimitiveArray::new(new_vals, arr.validity().cloned()))
}
