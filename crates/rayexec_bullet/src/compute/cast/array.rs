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
use crate::array::{ArrayData, ArrayOld};
use crate::bitmap::Bitmap;
use crate::datatype::{DataTypeOld, TimeUnit};
use crate::executor::builder::{ArrayBuilder, BooleanBuffer, GermanVarlenBuffer, PrimitiveBuffer};
use crate::executor::physical_type::{
    PhysicalBoolOld,
    PhysicalF16Old,
    PhysicalF32Old,
    PhysicalF64Old,
    PhysicalI128Old,
    PhysicalI16Old,
    PhysicalI32Old,
    PhysicalI64Old,
    PhysicalI8Old,
    PhysicalStorageOld,
    PhysicalU128Old,
    PhysicalU16Old,
    PhysicalU32Old,
    PhysicalU64Old,
    PhysicalU8Old,
    PhysicalUtf8Old,
};
use crate::executor::scalar::UnaryExecutor;
use crate::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use crate::storage::{AddressableStorage, PrimitiveStorage};

pub fn cast_array(arr: &ArrayOld, to: DataTypeOld, behavior: CastFailBehavior) -> Result<ArrayOld> {
    if arr.datatype() == &to {
        // TODO: Cow?
        return Ok(arr.clone());
    }

    let arr = match arr.datatype() {
        DataTypeOld::Null => {
            // Can cast NULL to anything else.
            let data = to.physical_type()?.zeroed_array_data(arr.logical_len());
            let validity = Bitmap::new_with_all_false(arr.logical_len());
            ArrayOld::new_with_validity_and_array_data(to, validity, data)
        }

        // String to anything else.
        DataTypeOld::Utf8 => cast_from_utf8(arr, to, behavior)?,

        // Primitive numerics to other primitive numerics.
        DataTypeOld::Int8 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI8Old>(arr, to, behavior)?
        }
        DataTypeOld::Int16 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI16Old>(arr, to, behavior)?
        }
        DataTypeOld::Int32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI32Old>(arr, to, behavior)?
        }
        DataTypeOld::Int64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI64Old>(arr, to, behavior)?
        }
        DataTypeOld::Int128 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI128Old>(arr, to, behavior)?
        }
        DataTypeOld::UInt8 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU8Old>(arr, to, behavior)?
        }
        DataTypeOld::UInt16 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU16Old>(arr, to, behavior)?
        }
        DataTypeOld::UInt32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU32Old>(arr, to, behavior)?
        }
        DataTypeOld::UInt64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU64Old>(arr, to, behavior)?
        }
        DataTypeOld::UInt128 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU128Old>(arr, to, behavior)?
        }
        DataTypeOld::Float16 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalF16Old>(arr, to, behavior)?
        }
        DataTypeOld::Float32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalF32Old>(arr, to, behavior)?
        }
        DataTypeOld::Float64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalF64Old>(arr, to, behavior)?
        }

        // Int to date32
        DataTypeOld::Int8 if to == DataTypeOld::Date32 => {
            cast_primitive_numeric::<PhysicalI8Old, i32>(arr, to, behavior)?
        }
        DataTypeOld::Int16 if to == DataTypeOld::Date32 => {
            cast_primitive_numeric::<PhysicalI16Old, i32>(arr, to, behavior)?
        }
        DataTypeOld::Int32 if to == DataTypeOld::Date32 => {
            cast_primitive_numeric::<PhysicalI32Old, i32>(arr, to, behavior)?
        }
        DataTypeOld::UInt8 if to == DataTypeOld::Date32 => {
            cast_primitive_numeric::<PhysicalU8Old, i32>(arr, to, behavior)?
        }
        DataTypeOld::UInt16 if to == DataTypeOld::Date32 => {
            cast_primitive_numeric::<PhysicalU16Old, i32>(arr, to, behavior)?
        }

        // Int to decimal.
        DataTypeOld::Int8 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI8Old>(arr, to, behavior)?
        }
        DataTypeOld::Int16 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI16Old>(arr, to, behavior)?
        }
        DataTypeOld::Int32 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI32Old>(arr, to, behavior)?
        }
        DataTypeOld::Int64 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI64Old>(arr, to, behavior)?
        }
        DataTypeOld::Int128 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI128Old>(arr, to, behavior)?
        }
        DataTypeOld::UInt8 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU8Old>(arr, to, behavior)?
        }
        DataTypeOld::UInt16 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU16Old>(arr, to, behavior)?
        }
        DataTypeOld::UInt32 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU32Old>(arr, to, behavior)?
        }
        DataTypeOld::UInt64 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU64Old>(arr, to, behavior)?
        }
        DataTypeOld::UInt128 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU128Old>(arr, to, behavior)?
        }

        // Float to decimal.
        DataTypeOld::Float32 if to.is_decimal() => {
            cast_float_to_decimal_helper::<PhysicalF32Old>(arr, to, behavior)?
        }
        DataTypeOld::Float64 if to.is_decimal() => {
            cast_float_to_decimal_helper::<PhysicalF64Old>(arr, to, behavior)?
        }

        // Decimal to decimal
        DataTypeOld::Decimal64(_) if to.is_decimal() => {
            decimal_rescale_helper::<PhysicalI64Old>(arr, to, behavior)?
        }
        DataTypeOld::Decimal128(_) if to.is_decimal() => {
            decimal_rescale_helper::<PhysicalI128Old>(arr, to, behavior)?
        }

        // Decimal to float.
        DataTypeOld::Decimal64(_) => match to {
            DataTypeOld::Float32 => {
                cast_decimal_to_float::<PhysicalI64Old, f32>(arr, to, behavior)?
            }
            DataTypeOld::Float64 => {
                cast_decimal_to_float::<PhysicalI64Old, f64>(arr, to, behavior)?
            }
            other => return Err(RayexecError::new(format!("Unhandled data type: {other}"))),
        },
        DataTypeOld::Decimal128(_) => match to {
            DataTypeOld::Float32 => {
                cast_decimal_to_float::<PhysicalI128Old, f32>(arr, to, behavior)?
            }
            DataTypeOld::Float64 => {
                cast_decimal_to_float::<PhysicalI128Old, f64>(arr, to, behavior)?
            }
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
    arr: &'a ArrayOld,
    to: DataTypeOld,
    behavior: CastFailBehavior,
) -> Result<ArrayOld>
where
    S: PhysicalStorageOld,
    S::Type<'a>: PrimInt,
{
    match to {
        DataTypeOld::Decimal64(_) => decimal_rescale::<S, Decimal64Type>(arr, to, behavior),
        DataTypeOld::Decimal128(_) => decimal_rescale::<S, Decimal128Type>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

pub fn decimal_rescale<'a, S, D>(
    arr: &'a ArrayOld,
    to: DataTypeOld,
    behavior: CastFailBehavior,
) -> Result<ArrayOld>
where
    S: PhysicalStorageOld,
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
    arr: &'a ArrayOld,
    to: DataTypeOld,
    behavior: CastFailBehavior,
) -> Result<ArrayOld>
where
    S: PhysicalStorageOld,
    S::Type<'a>: Float,
{
    match to {
        DataTypeOld::Decimal64(_) => cast_float_to_decimal::<S, Decimal64Type>(arr, to, behavior),
        DataTypeOld::Decimal128(_) => cast_float_to_decimal::<S, Decimal128Type>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

fn cast_float_to_decimal<'a, S, D>(
    arr: &'a ArrayOld,
    to: DataTypeOld,
    behavior: CastFailBehavior,
) -> Result<ArrayOld>
where
    S: PhysicalStorageOld,
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
    arr: &'a ArrayOld,
    to: DataTypeOld,
    behavior: CastFailBehavior,
) -> Result<ArrayOld>
where
    S: PhysicalStorageOld,
    F: Float + Default + Copy,
    <<S as PhysicalStorageOld>::Storage<'a> as AddressableStorage>::T: ToPrimitive,
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
    arr: &'a ArrayOld,
    to: DataTypeOld,
    behavior: CastFailBehavior,
) -> Result<ArrayOld>
where
    S: PhysicalStorageOld,
    S::Type<'a>: PrimInt,
{
    match to {
        DataTypeOld::Decimal64(_) => cast_int_to_decimal::<S, Decimal64Type>(arr, to, behavior),
        DataTypeOld::Decimal128(_) => cast_int_to_decimal::<S, Decimal128Type>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

fn cast_int_to_decimal<'a, S, D>(
    arr: &'a ArrayOld,
    to: DataTypeOld,
    behavior: CastFailBehavior,
) -> Result<ArrayOld>
where
    S: PhysicalStorageOld,
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
    arr: &'a ArrayOld,
    to: DataTypeOld,
    behavior: CastFailBehavior,
) -> Result<ArrayOld>
where
    S: PhysicalStorageOld,
    S::Type<'a>: ToPrimitive,
{
    match to {
        DataTypeOld::Int8 => cast_primitive_numeric::<S, i8>(arr, to, behavior),
        DataTypeOld::Int16 => cast_primitive_numeric::<S, i16>(arr, to, behavior),
        DataTypeOld::Int32 => cast_primitive_numeric::<S, i32>(arr, to, behavior),
        DataTypeOld::Int64 => cast_primitive_numeric::<S, i64>(arr, to, behavior),
        DataTypeOld::Int128 => cast_primitive_numeric::<S, i128>(arr, to, behavior),
        DataTypeOld::UInt8 => cast_primitive_numeric::<S, u8>(arr, to, behavior),
        DataTypeOld::UInt16 => cast_primitive_numeric::<S, u16>(arr, to, behavior),
        DataTypeOld::UInt32 => cast_primitive_numeric::<S, u32>(arr, to, behavior),
        DataTypeOld::UInt64 => cast_primitive_numeric::<S, u64>(arr, to, behavior),
        DataTypeOld::UInt128 => cast_primitive_numeric::<S, u128>(arr, to, behavior),
        DataTypeOld::Float16 => cast_primitive_numeric::<S, f16>(arr, to, behavior),
        DataTypeOld::Float32 => cast_primitive_numeric::<S, f32>(arr, to, behavior),
        DataTypeOld::Float64 => cast_primitive_numeric::<S, f64>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

pub fn cast_primitive_numeric<'a, S, T>(
    arr: &'a ArrayOld,
    datatype: DataTypeOld,
    behavior: CastFailBehavior,
) -> Result<ArrayOld>
where
    S: PhysicalStorageOld,
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
    arr: &ArrayOld,
    datatype: DataTypeOld,
    behavior: CastFailBehavior,
) -> Result<ArrayOld> {
    match datatype {
        DataTypeOld::Boolean => cast_parse_bool(arr, behavior),
        DataTypeOld::Int8 => cast_parse_primitive(arr, datatype, behavior, Int8Parser::default()),
        DataTypeOld::Int16 => cast_parse_primitive(arr, datatype, behavior, Int16Parser::default()),
        DataTypeOld::Int32 => cast_parse_primitive(arr, datatype, behavior, Int32Parser::default()),
        DataTypeOld::Int64 => cast_parse_primitive(arr, datatype, behavior, Int64Parser::default()),
        DataTypeOld::Int128 => {
            cast_parse_primitive(arr, datatype, behavior, Int128Parser::default())
        }
        DataTypeOld::UInt8 => cast_parse_primitive(arr, datatype, behavior, UInt8Parser::default()),
        DataTypeOld::UInt16 => {
            cast_parse_primitive(arr, datatype, behavior, UInt16Parser::default())
        }
        DataTypeOld::UInt32 => {
            cast_parse_primitive(arr, datatype, behavior, UInt32Parser::default())
        }
        DataTypeOld::UInt64 => {
            cast_parse_primitive(arr, datatype, behavior, UInt64Parser::default())
        }
        DataTypeOld::UInt128 => {
            cast_parse_primitive(arr, datatype, behavior, UInt128Parser::default())
        }
        DataTypeOld::Float16 => {
            cast_parse_primitive(arr, datatype, behavior, Float16Parser::default())
        }
        DataTypeOld::Float32 => {
            cast_parse_primitive(arr, datatype, behavior, Float32Parser::default())
        }
        DataTypeOld::Float64 => {
            cast_parse_primitive(arr, datatype, behavior, Float64Parser::default())
        }
        DataTypeOld::Decimal64(m) => cast_parse_primitive(
            arr,
            datatype,
            behavior,
            Decimal64Parser::new(m.precision, m.scale),
        ),
        DataTypeOld::Decimal128(m) => cast_parse_primitive(
            arr,
            datatype,
            behavior,
            Decimal128Parser::new(m.precision, m.scale),
        ),
        DataTypeOld::Date32 => cast_parse_primitive(arr, datatype, behavior, Date32Parser),
        DataTypeOld::Interval => {
            cast_parse_primitive(arr, datatype, behavior, IntervalParser::default())
        }
        other => Err(RayexecError::new(format!(
            "Unable to cast utf8 array to {other}"
        ))),
    }
}

pub fn cast_to_utf8(arr: &ArrayOld, behavior: CastFailBehavior) -> Result<ArrayOld> {
    match arr.datatype() {
        DataTypeOld::Boolean => {
            cast_format::<PhysicalBoolOld, _>(arr, BoolFormatter::default(), behavior)
        }
        DataTypeOld::Int8 => {
            cast_format::<PhysicalI8Old, _>(arr, Int8Formatter::default(), behavior)
        }
        DataTypeOld::Int16 => {
            cast_format::<PhysicalI16Old, _>(arr, Int16Formatter::default(), behavior)
        }
        DataTypeOld::Int32 => {
            cast_format::<PhysicalI32Old, _>(arr, Int32Formatter::default(), behavior)
        }
        DataTypeOld::Int64 => {
            cast_format::<PhysicalI64Old, _>(arr, Int64Formatter::default(), behavior)
        }
        DataTypeOld::Int128 => {
            cast_format::<PhysicalI128Old, _>(arr, Int128Formatter::default(), behavior)
        }
        DataTypeOld::UInt8 => {
            cast_format::<PhysicalU8Old, _>(arr, UInt8Formatter::default(), behavior)
        }
        DataTypeOld::UInt16 => {
            cast_format::<PhysicalU16Old, _>(arr, UInt16Formatter::default(), behavior)
        }
        DataTypeOld::UInt32 => {
            cast_format::<PhysicalU32Old, _>(arr, UInt32Formatter::default(), behavior)
        }
        DataTypeOld::UInt64 => {
            cast_format::<PhysicalU64Old, _>(arr, UInt64Formatter::default(), behavior)
        }
        DataTypeOld::UInt128 => {
            cast_format::<PhysicalU128Old, _>(arr, UInt128Formatter::default(), behavior)
        }
        DataTypeOld::Float32 => {
            cast_format::<PhysicalF32Old, _>(arr, Float32Formatter::default(), behavior)
        }
        DataTypeOld::Float64 => {
            cast_format::<PhysicalF64Old, _>(arr, Float64Formatter::default(), behavior)
        }
        DataTypeOld::Decimal64(m) => cast_format::<PhysicalI64Old, _>(
            arr,
            Decimal64Formatter::new(m.precision, m.scale),
            behavior,
        ),
        DataTypeOld::Decimal128(m) => cast_format::<PhysicalI128Old, _>(
            arr,
            Decimal128Formatter::new(m.precision, m.scale),
            behavior,
        ),
        DataTypeOld::Timestamp(m) => match m.unit {
            TimeUnit::Second => cast_format::<PhysicalI64Old, _>(
                arr,
                TimestampSecondsFormatter::default(),
                behavior,
            ),
            TimeUnit::Millisecond => cast_format::<PhysicalI64Old, _>(
                arr,
                TimestampMillisecondsFormatter::default(),
                behavior,
            ),
            TimeUnit::Microsecond => cast_format::<PhysicalI64Old, _>(
                arr,
                TimestampMicrosecondsFormatter::default(),
                behavior,
            ),
            TimeUnit::Nanosecond => cast_format::<PhysicalI64Old, _>(
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
    arr: &'a ArrayOld,
    mut formatter: F,
    behavior: CastFailBehavior,
) -> Result<ArrayOld>
where
    S: PhysicalStorageOld,
    F: Formatter<Type = S::Type<'a>>,
{
    let mut fail_state = behavior.new_state_for_array(arr);
    let mut string_buf = String::new();

    let output = UnaryExecutor::execute::<S, _, _>(
        arr,
        ArrayBuilder {
            datatype: DataTypeOld::Utf8,
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

fn cast_parse_bool(arr: &ArrayOld, behavior: CastFailBehavior) -> Result<ArrayOld> {
    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<PhysicalUtf8Old, _, _>(
        arr,
        ArrayBuilder {
            datatype: DataTypeOld::Boolean,
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
    arr: &ArrayOld,
    datatype: DataTypeOld,
    behavior: CastFailBehavior,
    mut parser: P,
) -> Result<ArrayOld>
where
    T: Default + Copy,
    P: Parser<Type = T>,
    ArrayData: From<PrimitiveStorage<T>>,
{
    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<PhysicalUtf8Old, _, _>(
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
    use crate::datatype::DecimalTypeMeta;
    use crate::scalar::ScalarValue;

    #[test]
    fn array_cast_utf8_to_i32() {
        let arr = ArrayOld::from_iter(["13", "18", "123456789"]);

        let got = cast_array(&arr, DataTypeOld::Int32, CastFailBehavior::Error).unwrap();

        assert_eq!(ScalarValue::from(13), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(18), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(123456789), got.logical_value(2).unwrap());
    }

    #[test]
    fn array_cast_utf8_to_i32_overflow_error() {
        let arr = ArrayOld::from_iter(["13", "18", "123456789000000"]);
        cast_array(&arr, DataTypeOld::Int32, CastFailBehavior::Error).unwrap_err();
    }

    #[test]
    fn array_cast_utf8_to_i32_overflow_null() {
        let arr = ArrayOld::from_iter(["13", "18", "123456789000000"]);

        let got = cast_array(&arr, DataTypeOld::Int32, CastFailBehavior::Null).unwrap();

        assert_eq!(ScalarValue::from(13), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(18), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::Null, got.logical_value(2).unwrap());
    }

    #[test]
    fn array_cast_null_to_f32() {
        let arr = ArrayOld::new_untyped_null_array(3);

        let got = cast_array(&arr, DataTypeOld::Float32, CastFailBehavior::Error).unwrap();

        assert_eq!(&DataTypeOld::Float32, got.datatype());

        assert_eq!(ScalarValue::Null, got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::Null, got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::Null, got.logical_value(2).unwrap());
    }

    #[test]
    fn array_cast_decimal64_to_f64() {
        let arr = ArrayOld::new_with_array_data(
            DataTypeOld::Decimal64(DecimalTypeMeta {
                precision: 10,
                scale: 3,
            }),
            PrimitiveStorage::from(vec![1500_i64, 2000_i64, 2500_i64]),
        );

        let got = cast_array(&arr, DataTypeOld::Float64, CastFailBehavior::Error).unwrap();

        assert_eq!(ScalarValue::Float64(1.5), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::Float64(2.0), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::Float64(2.5), got.logical_value(2).unwrap());
    }
}
