use std::ops::Mul;

use half::f16;
use num::{CheckedDiv, CheckedMul, Float, NumCast, PrimInt, ToPrimitive};
use rayexec_error::{RayexecError, Result};

use super::behavior::CastFailBehavior;
use super::format::{
    BoolFormatter,
    Decimal128Formatter,
    Decimal64Formatter,
    Float32Formatter,
    Float64Formatter,
    Formatter,
    Int128Formatter,
    Int16Formatter,
    Int32Formatter,
    Int64Formatter,
    Int8Formatter,
    TimestampMicrosecondsFormatter,
    TimestampMillisecondsFormatter,
    TimestampNanosecondsFormatter,
    TimestampSecondsFormatter,
    UInt128Formatter,
    UInt16Formatter,
    UInt32Formatter,
    UInt64Formatter,
    UInt8Formatter,
};
use super::parse::{
    BoolParser,
    Date32Parser,
    Decimal128Parser,
    Decimal64Parser,
    Float16Parser,
    Float32Parser,
    Float64Parser,
    Int128Parser,
    Int16Parser,
    Int32Parser,
    Int64Parser,
    Int8Parser,
    IntervalParser,
    Parser,
    UInt128Parser,
    UInt16Parser,
    UInt32Parser,
    UInt64Parser,
    UInt8Parser,
};
use crate::arrays::array::{Array, ArrayData};
use crate::arrays::bitmap::Bitmap;
use crate::arrays::datatype::{DataType, TimeUnit};
use crate::arrays::executor::builder::{
    ArrayBuilder,
    BooleanBuffer,
    GermanVarlenBuffer,
    PrimitiveBuffer,
};
use crate::arrays::executor::physical_type::{
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalStorage,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUtf8,
};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use crate::arrays::storage::{AddressableStorage, PrimitiveStorage};

pub fn cast_array(arr: &Array, to: DataType, behavior: CastFailBehavior) -> Result<Array> {
    if arr.datatype() == &to {
        // TODO: Cow?
        return Ok(arr.clone());
    }

    let arr = match arr.datatype() {
        DataType::Null => {
            // Can cast NULL to anything else.
            let data = to.physical_type()?.zeroed_array_data(arr.logical_len());
            let validity = Bitmap::new_with_all_false(arr.logical_len());
            Array::new_with_validity_and_array_data(to, validity, data)
        }

        // String to anything else.
        DataType::Utf8 => cast_from_utf8(arr, to, behavior)?,

        // Primitive numerics to other primitive numerics.
        DataType::Int8 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI8>(arr, to, behavior)?
        }
        DataType::Int16 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI16>(arr, to, behavior)?
        }
        DataType::Int32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI32>(arr, to, behavior)?
        }
        DataType::Int64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI64>(arr, to, behavior)?
        }
        DataType::Int128 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI128>(arr, to, behavior)?
        }
        DataType::UInt8 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU8>(arr, to, behavior)?
        }
        DataType::UInt16 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU16>(arr, to, behavior)?
        }
        DataType::UInt32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU32>(arr, to, behavior)?
        }
        DataType::UInt64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU64>(arr, to, behavior)?
        }
        DataType::UInt128 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU128>(arr, to, behavior)?
        }
        DataType::Float16 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalF16>(arr, to, behavior)?
        }
        DataType::Float32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalF32>(arr, to, behavior)?
        }
        DataType::Float64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalF64>(arr, to, behavior)?
        }

        // Int to date32
        DataType::Int8 if to == DataType::Date32 => {
            cast_primitive_numeric::<PhysicalI8, i32>(arr, to, behavior)?
        }
        DataType::Int16 if to == DataType::Date32 => {
            cast_primitive_numeric::<PhysicalI16, i32>(arr, to, behavior)?
        }
        DataType::Int32 if to == DataType::Date32 => {
            cast_primitive_numeric::<PhysicalI32, i32>(arr, to, behavior)?
        }
        DataType::UInt8 if to == DataType::Date32 => {
            cast_primitive_numeric::<PhysicalU8, i32>(arr, to, behavior)?
        }
        DataType::UInt16 if to == DataType::Date32 => {
            cast_primitive_numeric::<PhysicalU16, i32>(arr, to, behavior)?
        }

        // Int to decimal.
        DataType::Int8 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI8>(arr, to, behavior)?
        }
        DataType::Int16 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI16>(arr, to, behavior)?
        }
        DataType::Int32 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI32>(arr, to, behavior)?
        }
        DataType::Int64 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI64>(arr, to, behavior)?
        }
        DataType::Int128 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI128>(arr, to, behavior)?
        }
        DataType::UInt8 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU8>(arr, to, behavior)?
        }
        DataType::UInt16 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU16>(arr, to, behavior)?
        }
        DataType::UInt32 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU32>(arr, to, behavior)?
        }
        DataType::UInt64 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU64>(arr, to, behavior)?
        }
        DataType::UInt128 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU128>(arr, to, behavior)?
        }

        // Float to decimal.
        DataType::Float32 if to.is_decimal() => {
            cast_float_to_decimal_helper::<PhysicalF32>(arr, to, behavior)?
        }
        DataType::Float64 if to.is_decimal() => {
            cast_float_to_decimal_helper::<PhysicalF64>(arr, to, behavior)?
        }

        // Decimal to decimal
        DataType::Decimal64(_) if to.is_decimal() => {
            decimal_rescale_helper::<PhysicalI64>(arr, to, behavior)?
        }
        DataType::Decimal128(_) if to.is_decimal() => {
            decimal_rescale_helper::<PhysicalI128>(arr, to, behavior)?
        }

        // Decimal to float.
        DataType::Decimal64(_) => match to {
            DataType::Float32 => cast_decimal_to_float::<PhysicalI64, f32>(arr, to, behavior)?,
            DataType::Float64 => cast_decimal_to_float::<PhysicalI64, f64>(arr, to, behavior)?,
            other => return Err(RayexecError::new(format!("Unhandled data type: {other}"))),
        },
        DataType::Decimal128(_) => match to {
            DataType::Float32 => cast_decimal_to_float::<PhysicalI128, f32>(arr, to, behavior)?,
            DataType::Float64 => cast_decimal_to_float::<PhysicalI128, f64>(arr, to, behavior)?,
            other => return Err(RayexecError::new(format!("Unhandled data type: {other}"))),
        },

        // Anything to string.
        _ if to.is_utf8() => cast_to_utf8(arr, behavior)?,

        other => {
            return Err(RayexecError::new(format!(
                "Casting from {other} to {to} not implemented"
            )))
        }
    };

    Ok(arr)
}

fn decimal_rescale_helper<'a, S>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage,
    S::Type<'a>: PrimInt,
{
    match to {
        DataType::Decimal64(_) => decimal_rescale::<S, Decimal64Type>(arr, to, behavior),
        DataType::Decimal128(_) => decimal_rescale::<S, Decimal128Type>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

pub fn decimal_rescale<'a, S, D>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage,
    D: DecimalType,
    S::Type<'a>: PrimInt,
    ArrayData: From<PrimitiveStorage<D::Primitive>>,
{
    let new_meta = to.try_get_decimal_type_meta()?;
    let arr_meta = arr.datatype().try_get_decimal_type_meta()?;

    let scale_amount = <D::Primitive as NumCast>::from(
        10.pow((arr_meta.scale - new_meta.scale).unsigned_abs() as u32),
    )
    .expect("to be in range");

    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<S, _, _>(
        arr,
        ArrayBuilder {
            datatype: to,
            buffer: PrimitiveBuffer::with_len(arr.logical_len()),
        },
        |v, buf| {
            // Convert to decimal primitive.
            let v = match <D::Primitive as NumCast>::from(v) {
                Some(v) => v,
                None => {
                    fail_state.set_did_fail(buf.idx);
                    return;
                }
            };

            if arr_meta.scale < new_meta.scale {
                match v.checked_mul(&scale_amount) {
                    Some(v) => buf.put(&v),
                    None => fail_state.set_did_fail(buf.idx),
                }
            } else {
                match v.checked_div(&scale_amount) {
                    Some(v) => buf.put(&v),
                    None => fail_state.set_did_fail(buf.idx),
                }
            }
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

fn cast_float_to_decimal_helper<'a, S>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage,
    S::Type<'a>: Float,
{
    match to {
        DataType::Decimal64(_) => cast_float_to_decimal::<S, Decimal64Type>(arr, to, behavior),
        DataType::Decimal128(_) => cast_float_to_decimal::<S, Decimal128Type>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

fn cast_float_to_decimal<'a, S, D>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage,
    D: DecimalType,
    S::Type<'a>: Float,
    ArrayData: From<PrimitiveStorage<D::Primitive>>,
{
    let decimal_meta = to.try_get_decimal_type_meta()?;
    let scale = decimal_meta.scale;
    let precision = decimal_meta.precision;

    let scale = <<S::Storage<'a> as AddressableStorage>::T as NumCast>::from(
        10.pow(scale.unsigned_abs() as u32),
    )
    .ok_or_else(|| RayexecError::new(format!("Failed to cast scale {scale} to float")))?;

    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<S, _, _>(
        arr,
        ArrayBuilder {
            datatype: to,
            buffer: PrimitiveBuffer::with_len(arr.logical_len()),
        },
        |v, buf| {
            // TODO: Properly handle negative scale.
            let scaled_value = v.mul(scale).round();

            match <D::Primitive as NumCast>::from(scaled_value) {
                Some(v) => {
                    if let Err(err) = D::validate_precision(v, precision) {
                        fail_state.set_did_fail_with_error(buf.idx, err);
                        return;
                    }
                    buf.put(&v)
                }
                None => fail_state.set_did_fail(buf.idx),
            }
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

// TODO: Weird to specify both the float generic and datatype.
pub fn cast_decimal_to_float<'a, S, F>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage,
    F: Float + Default + Copy,
    <<S as PhysicalStorage>::Storage<'a> as AddressableStorage>::T: ToPrimitive,
    ArrayData: From<PrimitiveStorage<F>>,
{
    let decimal_meta = arr.datatype().try_get_decimal_type_meta()?;

    let scale = <F as NumCast>::from((10.0).powi(decimal_meta.scale as i32)).ok_or_else(|| {
        RayexecError::new(format!(
            "Failed to cast scale {} to float",
            decimal_meta.scale
        ))
    })?;

    let builder = ArrayBuilder {
        datatype: to,
        buffer: PrimitiveBuffer::<F>::with_len(arr.logical_len()),
    };

    let mut fail_state = behavior.new_state_for_array(arr);
    let output =
        UnaryExecutor::execute::<S, _, _>(arr, builder, |v, buf| match <F as NumCast>::from(v) {
            Some(v) => {
                let scaled = v.div(scale);
                buf.put(&scaled);
            }
            None => fail_state.set_did_fail(buf.idx),
        })?;

    fail_state.check_and_apply(arr, output)
}

fn cast_int_to_decimal_helper<'a, S>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage,
    S::Type<'a>: PrimInt,
{
    match to {
        DataType::Decimal64(_) => cast_int_to_decimal::<S, Decimal64Type>(arr, to, behavior),
        DataType::Decimal128(_) => cast_int_to_decimal::<S, Decimal128Type>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

fn cast_int_to_decimal<'a, S, D>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage,
    D: DecimalType,
    S::Type<'a>: PrimInt,
    ArrayData: From<PrimitiveStorage<D::Primitive>>,
{
    let decimal_meta = to.try_get_decimal_type_meta()?;
    let scale = decimal_meta.scale;
    let precision = decimal_meta.precision;

    let scale_amount = <D::Primitive as NumCast>::from(10.pow(scale.unsigned_abs() as u32))
        .expect("to be in range");

    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<S, _, _>(
        arr,
        ArrayBuilder {
            datatype: to,
            buffer: PrimitiveBuffer::with_len(arr.logical_len()),
        },
        |v, buf| {
            // Convert to decimal primitive.
            let v = match <D::Primitive as NumCast>::from(v) {
                Some(v) => v,
                None => {
                    fail_state.set_did_fail(buf.idx);
                    return;
                }
            };

            // Scale.
            let val = if scale > 0 {
                match v.checked_mul(&scale_amount) {
                    Some(v) => v,
                    None => {
                        fail_state.set_did_fail(buf.idx);
                        return;
                    }
                }
            } else {
                match v.checked_div(&scale_amount) {
                    Some(v) => v,
                    None => {
                        fail_state.set_did_fail(buf.idx);
                        return;
                    }
                }
            };

            if let Err(err) = D::validate_precision(val, precision) {
                fail_state.set_did_fail_with_error(buf.idx, err);
                return;
            }

            buf.put(&val);
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

fn cast_primitive_numeric_helper<'a, S>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage,
    S::Type<'a>: ToPrimitive,
{
    match to {
        DataType::Int8 => cast_primitive_numeric::<S, i8>(arr, to, behavior),
        DataType::Int16 => cast_primitive_numeric::<S, i16>(arr, to, behavior),
        DataType::Int32 => cast_primitive_numeric::<S, i32>(arr, to, behavior),
        DataType::Int64 => cast_primitive_numeric::<S, i64>(arr, to, behavior),
        DataType::Int128 => cast_primitive_numeric::<S, i128>(arr, to, behavior),
        DataType::UInt8 => cast_primitive_numeric::<S, u8>(arr, to, behavior),
        DataType::UInt16 => cast_primitive_numeric::<S, u16>(arr, to, behavior),
        DataType::UInt32 => cast_primitive_numeric::<S, u32>(arr, to, behavior),
        DataType::UInt64 => cast_primitive_numeric::<S, u64>(arr, to, behavior),
        DataType::UInt128 => cast_primitive_numeric::<S, u128>(arr, to, behavior),
        DataType::Float16 => cast_primitive_numeric::<S, f16>(arr, to, behavior),
        DataType::Float32 => cast_primitive_numeric::<S, f32>(arr, to, behavior),
        DataType::Float64 => cast_primitive_numeric::<S, f64>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

pub fn cast_primitive_numeric<'a, S, T>(
    arr: &'a Array,
    datatype: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage,
    S::Type<'a>: ToPrimitive,
    T: NumCast + Default + Copy,
    ArrayData: From<PrimitiveStorage<T>>,
{
    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<S, _, _>(
        arr,
        ArrayBuilder {
            datatype,
            buffer: PrimitiveBuffer::with_len(arr.logical_len()),
        },
        |v, buf| match T::from(v) {
            Some(v) => buf.put(&v),
            None => fail_state.set_did_fail(buf.idx),
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

pub fn cast_from_utf8(
    arr: &Array,
    datatype: DataType,
    behavior: CastFailBehavior,
) -> Result<Array> {
    match datatype {
        DataType::Boolean => cast_parse_bool(arr, behavior),
        DataType::Int8 => cast_parse_primitive(arr, datatype, behavior, Int8Parser::default()),
        DataType::Int16 => cast_parse_primitive(arr, datatype, behavior, Int16Parser::default()),
        DataType::Int32 => cast_parse_primitive(arr, datatype, behavior, Int32Parser::default()),
        DataType::Int64 => cast_parse_primitive(arr, datatype, behavior, Int64Parser::default()),
        DataType::Int128 => cast_parse_primitive(arr, datatype, behavior, Int128Parser::default()),
        DataType::UInt8 => cast_parse_primitive(arr, datatype, behavior, UInt8Parser::default()),
        DataType::UInt16 => cast_parse_primitive(arr, datatype, behavior, UInt16Parser::default()),
        DataType::UInt32 => cast_parse_primitive(arr, datatype, behavior, UInt32Parser::default()),
        DataType::UInt64 => cast_parse_primitive(arr, datatype, behavior, UInt64Parser::default()),
        DataType::UInt128 => {
            cast_parse_primitive(arr, datatype, behavior, UInt128Parser::default())
        }
        DataType::Float16 => {
            cast_parse_primitive(arr, datatype, behavior, Float16Parser::default())
        }
        DataType::Float32 => {
            cast_parse_primitive(arr, datatype, behavior, Float32Parser::default())
        }
        DataType::Float64 => {
            cast_parse_primitive(arr, datatype, behavior, Float64Parser::default())
        }
        DataType::Decimal64(m) => cast_parse_primitive(
            arr,
            datatype,
            behavior,
            Decimal64Parser::new(m.precision, m.scale),
        ),
        DataType::Decimal128(m) => cast_parse_primitive(
            arr,
            datatype,
            behavior,
            Decimal128Parser::new(m.precision, m.scale),
        ),
        DataType::Date32 => cast_parse_primitive(arr, datatype, behavior, Date32Parser),
        DataType::Interval => {
            cast_parse_primitive(arr, datatype, behavior, IntervalParser::default())
        }
        other => Err(RayexecError::new(format!(
            "Unable to cast utf8 array to {other}"
        ))),
    }
}

pub fn cast_to_utf8(arr: &Array, behavior: CastFailBehavior) -> Result<Array> {
    match arr.datatype() {
        DataType::Boolean => {
            cast_format::<PhysicalBool, _>(arr, BoolFormatter::default(), behavior)
        }
        DataType::Int8 => cast_format::<PhysicalI8, _>(arr, Int8Formatter::default(), behavior),
        DataType::Int16 => cast_format::<PhysicalI16, _>(arr, Int16Formatter::default(), behavior),
        DataType::Int32 => cast_format::<PhysicalI32, _>(arr, Int32Formatter::default(), behavior),
        DataType::Int64 => cast_format::<PhysicalI64, _>(arr, Int64Formatter::default(), behavior),
        DataType::Int128 => {
            cast_format::<PhysicalI128, _>(arr, Int128Formatter::default(), behavior)
        }
        DataType::UInt8 => cast_format::<PhysicalU8, _>(arr, UInt8Formatter::default(), behavior),
        DataType::UInt16 => {
            cast_format::<PhysicalU16, _>(arr, UInt16Formatter::default(), behavior)
        }
        DataType::UInt32 => {
            cast_format::<PhysicalU32, _>(arr, UInt32Formatter::default(), behavior)
        }
        DataType::UInt64 => {
            cast_format::<PhysicalU64, _>(arr, UInt64Formatter::default(), behavior)
        }
        DataType::UInt128 => {
            cast_format::<PhysicalU128, _>(arr, UInt128Formatter::default(), behavior)
        }
        DataType::Float32 => {
            cast_format::<PhysicalF32, _>(arr, Float32Formatter::default(), behavior)
        }
        DataType::Float64 => {
            cast_format::<PhysicalF64, _>(arr, Float64Formatter::default(), behavior)
        }
        DataType::Decimal64(m) => cast_format::<PhysicalI64, _>(
            arr,
            Decimal64Formatter::new(m.precision, m.scale),
            behavior,
        ),
        DataType::Decimal128(m) => cast_format::<PhysicalI128, _>(
            arr,
            Decimal128Formatter::new(m.precision, m.scale),
            behavior,
        ),
        DataType::Timestamp(m) => match m.unit {
            TimeUnit::Second => {
                cast_format::<PhysicalI64, _>(arr, TimestampSecondsFormatter::default(), behavior)
            }
            TimeUnit::Millisecond => cast_format::<PhysicalI64, _>(
                arr,
                TimestampMillisecondsFormatter::default(),
                behavior,
            ),
            TimeUnit::Microsecond => cast_format::<PhysicalI64, _>(
                arr,
                TimestampMicrosecondsFormatter::default(),
                behavior,
            ),
            TimeUnit::Nanosecond => cast_format::<PhysicalI64, _>(
                arr,
                TimestampNanosecondsFormatter::default(),
                behavior,
            ),
        },
        other => Err(RayexecError::new(format!(
            "Unable to cast {other} array to utf8"
        ))),
    }
}

fn cast_format<'a, S, F>(
    arr: &'a Array,
    mut formatter: F,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage,
    F: Formatter<Type = S::Type<'a>>,
{
    let mut fail_state = behavior.new_state_for_array(arr);
    let mut string_buf = String::new();

    let output = UnaryExecutor::execute::<S, _, _>(
        arr,
        ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::with_len(arr.logical_len()),
        },
        |v, buf| {
            string_buf.clear();
            match formatter.write(&v, &mut string_buf) {
                Ok(_) => buf.put(string_buf.as_str()),
                Err(_) => fail_state.set_did_fail(buf.idx),
            }
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

fn cast_parse_bool(arr: &Array, behavior: CastFailBehavior) -> Result<Array> {
    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<PhysicalUtf8, _, _>(
        arr,
        ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(arr.logical_len()),
        },
        |v, buf| match BoolParser.parse(v) {
            Some(v) => buf.put(&v),
            None => fail_state.set_did_fail(buf.idx),
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

fn cast_parse_primitive<P, T>(
    arr: &Array,
    datatype: DataType,
    behavior: CastFailBehavior,
    mut parser: P,
) -> Result<Array>
where
    T: Default + Copy,
    P: Parser<Type = T>,
    ArrayData: From<PrimitiveStorage<T>>,
{
    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<PhysicalUtf8, _, _>(
        arr,
        ArrayBuilder {
            datatype: datatype.clone(),
            buffer: PrimitiveBuffer::<T>::with_len(arr.logical_len()),
        },
        |v, buf| match parser.parse(v) {
            Some(v) => buf.put(&v),
            None => fail_state.set_did_fail(buf.idx),
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DecimalTypeMeta;
    use crate::arrays::scalar::ScalarValue;

    #[test]
    fn array_cast_utf8_to_i32() {
        let arr = Array::from_iter(["13", "18", "123456789"]);

        let got = cast_array(&arr, DataType::Int32, CastFailBehavior::Error).unwrap();

        assert_eq!(ScalarValue::from(13), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(18), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(123456789), got.logical_value(2).unwrap());
    }

    #[test]
    fn array_cast_utf8_to_i32_overflow_error() {
        let arr = Array::from_iter(["13", "18", "123456789000000"]);
        cast_array(&arr, DataType::Int32, CastFailBehavior::Error).unwrap_err();
    }

    #[test]
    fn array_cast_utf8_to_i32_overflow_null() {
        let arr = Array::from_iter(["13", "18", "123456789000000"]);

        let got = cast_array(&arr, DataType::Int32, CastFailBehavior::Null).unwrap();

        assert_eq!(ScalarValue::from(13), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(18), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::Null, got.logical_value(2).unwrap());
    }

    #[test]
    fn array_cast_null_to_f32() {
        let arr = Array::new_untyped_null_array(3);

        let got = cast_array(&arr, DataType::Float32, CastFailBehavior::Error).unwrap();

        assert_eq!(&DataType::Float32, got.datatype());

        assert_eq!(ScalarValue::Null, got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::Null, got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::Null, got.logical_value(2).unwrap());
    }

    #[test]
    fn array_cast_decimal64_to_f64() {
        let arr = Array::new_with_array_data(
            DataType::Decimal64(DecimalTypeMeta {
                precision: 10,
                scale: 3,
            }),
            PrimitiveStorage::from(vec![1500_i64, 2000_i64, 2500_i64]),
        );

        let got = cast_array(&arr, DataType::Float64, CastFailBehavior::Error).unwrap();

        assert_eq!(ScalarValue::Float64(1.5), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::Float64(2.0), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::Float64(2.5), got.logical_value(2).unwrap());
    }
}
