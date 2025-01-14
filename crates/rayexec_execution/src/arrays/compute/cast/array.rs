use std::ops::Mul;
use std::sync::Arc;

use num::{CheckedDiv, CheckedMul, Float, NumCast, PrimInt, ToPrimitive};
use rayexec_error::{RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

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
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::physical_type::{
    MutablePhysicalStorage,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalStorage,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUtf8,
};
use crate::arrays::array::validity::Validity;
use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, TimeUnit};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::arrays::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};

/// Casts an array to another array.
///
/// The datatype of `out` determines the what we're casting values to.
///
/// `behavior` determines what happens if casting results in an overflow or some
/// other precision/accuracy error. Note that if we don't have an implementation
/// of casting from one type to another, this will always error.
pub fn cast_array(
    arr: &mut Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()> {
    if arr.datatype() == out.datatype() {
        out.try_clone_from(&NopBufferManager, arr)?;
        out.select(&Arc::new(NopBufferManager), sel)?;

        return Ok(());
    }

    let to = out.datatype();

    match arr.datatype() {
        DataType::Null => {
            // Can cast NULL to anything else. Just set the valid mask to all
            // invalid.
            out.put_validity(Validity::new_all_invalid(out.capacity()))?;
            Ok(())
        }

        // String to anything else.
        DataType::Utf8 => cast_from_utf8(arr, sel, out, behavior),

        // Primitive numerics to other primitive numerics.
        DataType::Int8 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI8>(arr, sel, out, behavior)
        }
        DataType::Int16 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI16>(arr, sel, out, behavior)
        }
        DataType::Int32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI32>(arr, sel, out, behavior)
        }
        DataType::Int64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI64>(arr, sel, out, behavior)
        }
        DataType::Int128 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI128>(arr, sel, out, behavior)
        }
        DataType::UInt8 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU8>(arr, sel, out, behavior)
        }
        DataType::UInt16 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU16>(arr, sel, out, behavior)
        }
        DataType::UInt32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU32>(arr, sel, out, behavior)
        }
        DataType::UInt64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU64>(arr, sel, out, behavior)
        }
        DataType::UInt128 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU128>(arr, sel, out, behavior)
        }
        DataType::Float16 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalF16>(arr, sel, out, behavior)
        }
        DataType::Float32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalF32>(arr, sel, out, behavior)
        }
        DataType::Float64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalF64>(arr, sel, out, behavior)
        }

        // Int to date32
        DataType::Int8 if to == &DataType::Date32 => {
            cast_primitive_numeric::<PhysicalI8, PhysicalI32>(arr, sel, out, behavior)
        }
        DataType::Int16 if to == &DataType::Date32 => {
            cast_primitive_numeric::<PhysicalI16, PhysicalU32>(arr, sel, out, behavior)
        }
        DataType::Int32 if to == &DataType::Date32 => {
            cast_primitive_numeric::<PhysicalI32, PhysicalU32>(arr, sel, out, behavior)
        }
        DataType::UInt8 if to == &DataType::Date32 => {
            cast_primitive_numeric::<PhysicalU8, PhysicalU32>(arr, sel, out, behavior)
        }
        DataType::UInt16 if to == &DataType::Date32 => {
            cast_primitive_numeric::<PhysicalU16, PhysicalU32>(arr, sel, out, behavior)
        }

        // Int to decimal.
        DataType::Int8 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI8>(arr, sel, out, behavior)
        }
        DataType::Int16 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI16>(arr, sel, out, behavior)
        }
        DataType::Int32 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI32>(arr, sel, out, behavior)
        }
        DataType::Int64 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI64>(arr, sel, out, behavior)
        }
        DataType::Int128 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI128>(arr, sel, out, behavior)
        }
        DataType::UInt8 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU8>(arr, sel, out, behavior)
        }
        DataType::UInt16 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU16>(arr, sel, out, behavior)
        }
        DataType::UInt32 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU32>(arr, sel, out, behavior)
        }
        DataType::UInt64 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU64>(arr, sel, out, behavior)
        }
        DataType::UInt128 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU128>(arr, sel, out, behavior)
        }

        // Float to decimal.
        DataType::Float16 if to.is_decimal() => {
            cast_float_to_decimal_helper::<PhysicalF16>(arr, sel, out, behavior)
        }
        DataType::Float32 if to.is_decimal() => {
            cast_float_to_decimal_helper::<PhysicalF32>(arr, sel, out, behavior)
        }
        DataType::Float64 if to.is_decimal() => {
            cast_float_to_decimal_helper::<PhysicalF64>(arr, sel, out, behavior)
        }

        // Decimal to decimal
        DataType::Decimal64(_) if to.is_decimal() => {
            decimal_rescale_helper::<Decimal64Type>(arr, sel, out, behavior)
        }
        DataType::Decimal128(_) if to.is_decimal() => {
            decimal_rescale_helper::<Decimal128Type>(arr, sel, out, behavior)
        }

        // Decimal to float.
        DataType::Decimal64(_) => match to {
            DataType::Float16 => {
                cast_decimal_to_float::<Decimal64Type, PhysicalF16>(arr, sel, out, behavior)
            }
            DataType::Float32 => {
                cast_decimal_to_float::<Decimal64Type, PhysicalF32>(arr, sel, out, behavior)
            }
            DataType::Float64 => {
                cast_decimal_to_float::<Decimal64Type, PhysicalF64>(arr, sel, out, behavior)
            }
            other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
        },
        DataType::Decimal128(_) => match to {
            DataType::Float16 => {
                cast_decimal_to_float::<Decimal128Type, PhysicalF16>(arr, sel, out, behavior)
            }
            DataType::Float32 => {
                cast_decimal_to_float::<Decimal128Type, PhysicalF32>(arr, sel, out, behavior)
            }
            DataType::Float64 => {
                cast_decimal_to_float::<Decimal128Type, PhysicalF64>(arr, sel, out, behavior)
            }
            other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
        },

        // Anything to string.
        _ if to.is_utf8() => cast_to_utf8(arr, sel, out, behavior),

        other => Err(RayexecError::new(format!(
            "Casting from {other} to {to} not implemented",
        ))),
    }
}

fn decimal_rescale_helper<D1>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()>
where
    D1: DecimalType,
{
    match out.datatype() {
        DataType::Decimal64(_) => decimal_rescale::<D1, Decimal64Type>(arr, sel, out, behavior),
        DataType::Decimal128(_) => decimal_rescale::<D1, Decimal128Type>(arr, sel, out, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

pub fn decimal_rescale<D1, D2>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()>
where
    D1: DecimalType,
    D2: DecimalType,
{
    let new_meta = arr.datatype().try_get_decimal_type_meta()?;
    let arr_meta = arr.datatype().try_get_decimal_type_meta()?;

    let scale_amount = <D2::Primitive as NumCast>::from(
        10.pow((arr_meta.scale - new_meta.scale).unsigned_abs() as u32),
    )
    .expect("to be in range");

    let mut fail_state = behavior.new_state();
    UnaryExecutor::execute::<D1::Storage, D2::Storage, _>(
        arr,
        sel,
        OutBuffer::from_array(out)?,
        |&v, buf| {
            // Convert to decimal primitive.
            let v = match <D2::Primitive as NumCast>::from(v) {
                Some(v) => v,
                None => {
                    fail_state.set_error(|| RayexecError::new("Failed cast decimal"));
                    buf.put_null();
                    return;
                }
            };

            if arr_meta.scale < new_meta.scale {
                match v.checked_mul(&scale_amount) {
                    Some(v) => buf.put(&v),
                    None => {
                        fail_state.set_error(|| RayexecError::new("Failed cast decimal"));
                        buf.put_null();
                    }
                }
            } else {
                match v.checked_div(&scale_amount) {
                    Some(v) => buf.put(&v),
                    None => {
                        fail_state.set_error(|| RayexecError::new("Failed cast decimal"));
                        buf.put_null();
                    }
                }
            }
        },
    )?;

    fail_state.into_result()
}

fn cast_float_to_decimal_helper<S>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()>
where
    S: PhysicalStorage,
    S::StorageType: Float,
{
    match out.datatype() {
        DataType::Decimal64(_) => {
            cast_float_to_decimal::<S, Decimal64Type>(arr, sel, out, behavior)
        }
        DataType::Decimal128(_) => {
            cast_float_to_decimal::<S, Decimal128Type>(arr, sel, out, behavior)
        }
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

fn cast_float_to_decimal<S, D>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()>
where
    S: PhysicalStorage,
    S::StorageType: Float,
    D: DecimalType,
{
    let decimal_meta = out.datatype().try_get_decimal_type_meta()?;
    let scale = decimal_meta.scale;
    let precision = decimal_meta.precision;

    let scale = <S::StorageType as NumCast>::from(10.pow(scale.unsigned_abs() as u32))
        .ok_or_else(|| RayexecError::new(format!("Failed to cast scale {scale} to float")))?;

    let mut fail_state = behavior.new_state();
    UnaryExecutor::execute::<S, D::Storage, _>(
        arr,
        sel,
        OutBuffer::from_array(out)?,
        |&v, buf| {
            // TODO: Properly handle negative scale.
            let scaled_value = v.mul(scale).round();

            match <D::Primitive as NumCast>::from(scaled_value) {
                Some(v) => {
                    if let Err(err) = D::validate_precision(v, precision) {
                        fail_state.set_error(|| err);
                        buf.put_null();
                        return;
                    }
                    buf.put(&v)
                }
                None => {
                    fail_state.set_error(|| RayexecError::new("Failed cast decimal"));
                    buf.put_null();
                }
            }
        },
    )?;

    fail_state.into_result()
}

pub fn cast_decimal_to_float<D, S>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()>
where
    D: DecimalType,
    S: MutablePhysicalStorage,
    S::StorageType: Float + Copy,
{
    let decimal_meta = arr.datatype().try_get_decimal_type_meta()?;

    let scale = <S::StorageType as NumCast>::from((10.0).powi(decimal_meta.scale as i32))
        .ok_or_else(|| {
            RayexecError::new(format!(
                "Failed to cast scale {} to float",
                decimal_meta.scale
            ))
        })?;

    let mut fail_state = behavior.new_state();
    UnaryExecutor::execute::<D::Storage, S, _>(
        arr,
        sel,
        OutBuffer::from_array(out)?,
        |&v, buf| match <S::StorageType as NumCast>::from(v) {
            Some(v) => {
                let scaled = v / scale;
                buf.put(&scaled);
            }
            None => {
                fail_state.set_error(|| RayexecError::new("Failed to cast float to decimal"));
                buf.put_null();
            }
        },
    )?;

    fail_state.into_result()
}

fn cast_int_to_decimal_helper<S>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()>
where
    S: PhysicalStorage,
    S::StorageType: PrimInt,
{
    match out.datatype() {
        DataType::Decimal64(_) => cast_int_to_decimal::<S, Decimal64Type>(arr, sel, out, behavior),
        DataType::Decimal128(_) => {
            cast_int_to_decimal::<S, Decimal128Type>(arr, sel, out, behavior)
        }
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

fn cast_int_to_decimal<S, D>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()>
where
    S: PhysicalStorage,
    D: DecimalType,
    S::StorageType: PrimInt,
{
    let decimal_meta = out.datatype().try_get_decimal_type_meta()?;
    let scale = decimal_meta.scale;
    let precision = decimal_meta.precision;

    let scale_amount = <D::Primitive as NumCast>::from(10.pow(scale.unsigned_abs() as u32))
        .expect("to be in range");

    let mut fail_state = behavior.new_state();
    UnaryExecutor::execute::<S, D::Storage, _>(
        arr,
        sel,
        OutBuffer::from_array(out)?,
        |&v, buf| {
            // Convert to decimal primitive.
            let v = match <D::Primitive as NumCast>::from(v) {
                Some(v) => v,
                None => {
                    fail_state.set_error(|| RayexecError::new("Failed to cast int to decimal"));
                    buf.put_null();
                    return;
                }
            };

            // Scale.
            let val = if scale > 0 {
                match v.checked_mul(&scale_amount) {
                    Some(v) => v,
                    None => {
                        fail_state.set_error(|| RayexecError::new("Failed to cast int to decimal"));
                        buf.put_null();
                        return;
                    }
                }
            } else {
                match v.checked_div(&scale_amount) {
                    Some(v) => v,
                    None => {
                        fail_state.set_error(|| RayexecError::new("Failed to cast int to decimal"));
                        buf.put_null();
                        return;
                    }
                }
            };

            if let Err(err) = D::validate_precision(val, precision) {
                fail_state.set_error(|| err);
                buf.put_null();
                return;
            }

            buf.put(&val);
        },
    )?;

    fail_state.into_result()
}

fn cast_primitive_numeric_helper<S>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()>
where
    S: PhysicalStorage,
    S::StorageType: ToPrimitive + Sized + Copy,
{
    match out.datatype() {
        DataType::Int8 => cast_primitive_numeric::<S, PhysicalI8>(arr, sel, out, behavior),
        DataType::Int16 => cast_primitive_numeric::<S, PhysicalI16>(arr, sel, out, behavior),
        DataType::Int32 => cast_primitive_numeric::<S, PhysicalI32>(arr, sel, out, behavior),
        DataType::Int64 => cast_primitive_numeric::<S, PhysicalI64>(arr, sel, out, behavior),
        DataType::Int128 => cast_primitive_numeric::<S, PhysicalI128>(arr, sel, out, behavior),
        DataType::UInt8 => cast_primitive_numeric::<S, PhysicalU8>(arr, sel, out, behavior),
        DataType::UInt16 => cast_primitive_numeric::<S, PhysicalU16>(arr, sel, out, behavior),
        DataType::UInt32 => cast_primitive_numeric::<S, PhysicalU32>(arr, sel, out, behavior),
        DataType::UInt64 => cast_primitive_numeric::<S, PhysicalU64>(arr, sel, out, behavior),
        DataType::UInt128 => cast_primitive_numeric::<S, PhysicalU128>(arr, sel, out, behavior),
        DataType::Float16 => cast_primitive_numeric::<S, PhysicalF16>(arr, sel, out, behavior),
        DataType::Float32 => cast_primitive_numeric::<S, PhysicalF32>(arr, sel, out, behavior),
        DataType::Float64 => cast_primitive_numeric::<S, PhysicalF64>(arr, sel, out, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

/// Cast a primitive number to some other primitive numeric.
fn cast_primitive_numeric<S1, S2>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()>
where
    S1: PhysicalStorage,
    S1::StorageType: ToPrimitive + Sized + Copy,
    S2: MutablePhysicalStorage,
    S2::StorageType: NumCast + Copy,
{
    let mut fail_state = behavior.new_state();
    UnaryExecutor::execute::<S1, S2, _>(arr, sel, OutBuffer::from_array(out)?, |&v, buf| {
        match NumCast::from(v) {
            Some(v) => buf.put(&v),
            None => {
                fail_state.set_error(|| RayexecError::new("Failed to cast primitive numeric"));
                buf.put_null();
            }
        }
    })?;

    fail_state.into_result()
}

pub fn cast_from_utf8(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()> {
    match out.datatype() {
        DataType::Boolean => {
            cast_parse_primitive::<_, PhysicalBool>(arr, sel, out, behavior, BoolParser)
        }
        DataType::Int8 => {
            cast_parse_primitive::<_, PhysicalI8>(arr, sel, out, behavior, Int8Parser::default())
        }
        DataType::Int16 => {
            cast_parse_primitive::<_, PhysicalI16>(arr, sel, out, behavior, Int16Parser::default())
        }
        DataType::Int32 => {
            cast_parse_primitive::<_, PhysicalI32>(arr, sel, out, behavior, Int32Parser::default())
        }
        DataType::Int64 => {
            cast_parse_primitive::<_, PhysicalI64>(arr, sel, out, behavior, Int64Parser::default())
        }
        DataType::Int128 => cast_parse_primitive::<_, PhysicalI128>(
            arr,
            sel,
            out,
            behavior,
            Int128Parser::default(),
        ),
        DataType::UInt8 => {
            cast_parse_primitive::<_, PhysicalU8>(arr, sel, out, behavior, UInt8Parser::default())
        }
        DataType::UInt16 => {
            cast_parse_primitive::<_, PhysicalU16>(arr, sel, out, behavior, UInt16Parser::default())
        }
        DataType::UInt32 => {
            cast_parse_primitive::<_, PhysicalU32>(arr, sel, out, behavior, UInt32Parser::default())
        }
        DataType::UInt64 => {
            cast_parse_primitive::<_, PhysicalU64>(arr, sel, out, behavior, UInt64Parser::default())
        }
        DataType::UInt128 => cast_parse_primitive::<_, PhysicalU128>(
            arr,
            sel,
            out,
            behavior,
            UInt128Parser::default(),
        ),
        DataType::Float16 => cast_parse_primitive::<_, PhysicalF16>(
            arr,
            sel,
            out,
            behavior,
            Float16Parser::default(),
        ),
        DataType::Float32 => cast_parse_primitive::<_, PhysicalF32>(
            arr,
            sel,
            out,
            behavior,
            Float32Parser::default(),
        ),
        DataType::Float64 => cast_parse_primitive::<_, PhysicalF64>(
            arr,
            sel,
            out,
            behavior,
            Float64Parser::default(),
        ),
        DataType::Decimal64(m) => cast_parse_primitive::<_, PhysicalI64>(
            arr,
            sel,
            out,
            behavior,
            Decimal64Parser::new(m.precision, m.scale),
        ),
        DataType::Decimal128(m) => cast_parse_primitive::<_, PhysicalI128>(
            arr,
            sel,
            out,
            behavior,
            Decimal128Parser::new(m.precision, m.scale),
        ),
        DataType::Date32 => {
            cast_parse_primitive::<_, PhysicalI32>(arr, sel, out, behavior, Date32Parser)
        }
        DataType::Interval => cast_parse_primitive::<_, PhysicalInterval>(
            arr,
            sel,
            out,
            behavior,
            IntervalParser::default(),
        ),
        other => Err(RayexecError::new(format!(
            "Unable to cast utf8 array to {other}"
        ))),
    }
}

pub fn cast_to_utf8(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
) -> Result<()> {
    match arr.datatype() {
        DataType::Boolean => {
            cast_format::<PhysicalBool, _>(arr, sel, out, BoolFormatter::default(), behavior)
        }
        DataType::Int8 => {
            cast_format::<PhysicalI8, _>(arr, sel, out, Int8Formatter::default(), behavior)
        }
        DataType::Int16 => {
            cast_format::<PhysicalI16, _>(arr, sel, out, Int16Formatter::default(), behavior)
        }
        DataType::Int32 => {
            cast_format::<PhysicalI32, _>(arr, sel, out, Int32Formatter::default(), behavior)
        }
        DataType::Int64 => {
            cast_format::<PhysicalI64, _>(arr, sel, out, Int64Formatter::default(), behavior)
        }
        DataType::Int128 => {
            cast_format::<PhysicalI128, _>(arr, sel, out, Int128Formatter::default(), behavior)
        }
        DataType::UInt8 => {
            cast_format::<PhysicalU8, _>(arr, sel, out, UInt8Formatter::default(), behavior)
        }
        DataType::UInt16 => {
            cast_format::<PhysicalU16, _>(arr, sel, out, UInt16Formatter::default(), behavior)
        }
        DataType::UInt32 => {
            cast_format::<PhysicalU32, _>(arr, sel, out, UInt32Formatter::default(), behavior)
        }
        DataType::UInt64 => {
            cast_format::<PhysicalU64, _>(arr, sel, out, UInt64Formatter::default(), behavior)
        }
        DataType::UInt128 => {
            cast_format::<PhysicalU128, _>(arr, sel, out, UInt128Formatter::default(), behavior)
        }
        DataType::Float32 => {
            cast_format::<PhysicalF32, _>(arr, sel, out, Float32Formatter::default(), behavior)
        }
        DataType::Float64 => {
            cast_format::<PhysicalF64, _>(arr, sel, out, Float64Formatter::default(), behavior)
        }
        DataType::Decimal64(m) => cast_format::<PhysicalI64, _>(
            arr,
            sel,
            out,
            Decimal64Formatter::new(m.precision, m.scale),
            behavior,
        ),
        DataType::Decimal128(m) => cast_format::<PhysicalI128, _>(
            arr,
            sel,
            out,
            Decimal128Formatter::new(m.precision, m.scale),
            behavior,
        ),
        DataType::Timestamp(m) => match m.unit {
            TimeUnit::Second => cast_format::<PhysicalI64, _>(
                arr,
                sel,
                out,
                TimestampSecondsFormatter::default(),
                behavior,
            ),
            TimeUnit::Millisecond => cast_format::<PhysicalI64, _>(
                arr,
                sel,
                out,
                TimestampMillisecondsFormatter::default(),
                behavior,
            ),
            TimeUnit::Microsecond => cast_format::<PhysicalI64, _>(
                arr,
                sel,
                out,
                TimestampMicrosecondsFormatter::default(),
                behavior,
            ),
            TimeUnit::Nanosecond => cast_format::<PhysicalI64, _>(
                arr,
                sel,
                out,
                TimestampNanosecondsFormatter::default(),
                behavior,
            ),
        },
        other => Err(RayexecError::new(format!(
            "Unable to cast {other} array to utf8"
        ))),
    }
}

/// Cast an array to strings by formatting values.
fn cast_format<S, F>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    mut formatter: F,
    behavior: CastFailBehavior,
) -> Result<()>
where
    S: PhysicalStorage,
    F: Formatter<Type = S::StorageType>,
{
    let mut fail_state = behavior.new_state();
    let mut string_buf = String::new();

    UnaryExecutor::execute::<S, PhysicalUtf8, _>(
        arr,
        sel,
        OutBuffer::from_array(out)?,
        |v, buf| {
            string_buf.clear();
            match formatter.write(v, &mut string_buf) {
                Ok(_) => buf.put(string_buf.as_str()),
                Err(_) => {
                    fail_state.set_error(|| RayexecError::new("Failed to cast to utf8"));
                    buf.put_null();
                }
            }
        },
    )?;

    fail_state.into_result()
}

/// Cast a utf8 array to some other primitive type by parsing string values.
fn cast_parse_primitive<P, S>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
    behavior: CastFailBehavior,
    mut parser: P,
) -> Result<()>
where
    S::StorageType: Sized,
    P: Parser<Type = S::StorageType>,
    S: MutablePhysicalStorage,
{
    let mut fail_state = behavior.new_state();
    UnaryExecutor::execute::<PhysicalUtf8, S, _>(
        arr,
        sel,
        OutBuffer::from_array(out)?,
        |v, buf| match parser.parse(v) {
            Some(v) => buf.put(&v),
            None => {
                fail_state.set_error(|| RayexecError::new("Failed to parse value from utf8"));
                buf.put_null();
            }
        },
    )?;

    fail_state.into_result()
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::datatype::DecimalTypeMeta;
    use crate::arrays::testutil::assert_arrays_eq;

    #[test]
    fn array_cast_utf8_to_i32() {
        let mut arr = Array::try_from_iter(["13", "18", "123456789"]).unwrap();
        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Int32, 3).unwrap();

        cast_array(&mut arr, 0..3, &mut out, CastFailBehavior::Error).unwrap();

        let expected = Array::try_from_iter([13, 18, 123456789]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn array_cast_utf8_to_i32_overflow_error() {
        let mut arr = Array::try_from_iter(["13", "18", "123456789000000"]).unwrap();
        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Int32, 3).unwrap();
        cast_array(&mut arr, 0..3, &mut out, CastFailBehavior::Error).unwrap_err();
    }

    #[test]
    fn array_cast_utf8_to_i32_overflow_null() {
        let mut arr = Array::try_from_iter(["13", "18", "123456789000000"]).unwrap();
        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Int32, 3).unwrap();

        cast_array(&mut arr, 0..3, &mut out, CastFailBehavior::Null).unwrap();

        let expected = Array::try_from_iter([Some(13), Some(18), None]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn array_cast_null_to_f32() {
        let mut arr = Array::try_new(&Arc::new(NopBufferManager), DataType::Null, 3).unwrap();
        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Float32, 3).unwrap();

        cast_array(&mut arr, 0..3, &mut out, CastFailBehavior::Error).unwrap();

        let expected = Array::try_from_iter([None as Option<f32>, None, None]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn array_cast_decimal64_to_f64() {
        let mut arr = Array::try_from_iter([1500_i64, 2000, 2500]).unwrap();
        // '[1.500, 2.000, 2.500]'
        arr.datatype = DataType::Decimal64(DecimalTypeMeta::new(10, 3));

        let mut out = Array::try_new(&Arc::new(NopBufferManager), DataType::Float64, 3).unwrap();
        cast_array(&mut arr, 0..3, &mut out, CastFailBehavior::Error).unwrap();

        let expected = Array::try_from_iter([1.5_f64, 2.0, 2.5]).unwrap();
        assert_arrays_eq(&expected, &out);
    }
}
