use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::{Mul, Neg};

use glaredb_error::{DbError, Result};
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, Float, NumCast, PrimInt};

use super::null::NullToAnything;
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI128,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
    PhysicalUtf8,
    ScalarStorage,
};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::scalar::decimal::{
    Decimal64Type,
    Decimal128Type,
    DecimalPrimitive,
    DecimalType,
};
use crate::functions::cast::behavior::CastErrorState;
use crate::functions::cast::parse::{DecimalParser, Parser};
use crate::functions::cast::{
    CastFunction,
    CastFunctionSet,
    CastRule,
    RawCastFunction,
    TO_DECIMAL64_CAST_RULE,
    TO_DECIMAL128_CAST_RULE,
};
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_TO_DECIMAL64: CastFunctionSet = CastFunctionSet {
    name: "to_decimal64",
    target: DataTypeId::Decimal64,
    #[rustfmt::skip]
    functions: &[
        // Null -> Decimal64
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_DECIMAL64_CAST_RULE),
        // Utf8 -> Decimal64
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToDecimal::<Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        // Int_ -> Decimal64
        RawCastFunction::new(DataTypeId::Int8, &IntToDecimal::<PhysicalI8, Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int16, &IntToDecimal::<PhysicalI16, Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int32, &IntToDecimal::<PhysicalI32, Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int64, &IntToDecimal::<PhysicalI64, Decimal64Type>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &IntToDecimal::<PhysicalI128, Decimal64Type>::new(), CastRule::Explicit),
        // UInt_ -> Decimal64
        RawCastFunction::new(DataTypeId::UInt8, &IntToDecimal::<PhysicalU8, Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt16, &IntToDecimal::<PhysicalU16, Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt32, &IntToDecimal::<PhysicalU32, Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt64, &IntToDecimal::<PhysicalU64, Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt128, &IntToDecimal::<PhysicalU128, Decimal64Type>::new(), CastRule::Explicit),
        // Float_ -> Decimal64
        RawCastFunction::new(DataTypeId::Float16, &FloatToDecimal::<PhysicalF16, Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Float32, &FloatToDecimal::<PhysicalF32, Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Float64, &FloatToDecimal::<PhysicalF64, Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        // Decimal_ -> Decimal64 (rescale)
        RawCastFunction::new(DataTypeId::Decimal64, &DecimalToDecimal::<Decimal64Type, Decimal64Type>::new(), TO_DECIMAL64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Decimal128, &DecimalToDecimal::<Decimal128Type, Decimal64Type>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_DECIMAL128: CastFunctionSet = CastFunctionSet {
    name: "to_decimal128",
    target: DataTypeId::Decimal128,
    #[rustfmt::skip]
    functions: &[
        // Null -> Decimal128
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_DECIMAL128_CAST_RULE),
        // Utf8 -> Decimal128
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToDecimal::<Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        // Int_ -> Decimal128
        RawCastFunction::new(DataTypeId::Int8, &IntToDecimal::<PhysicalI8, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int16, &IntToDecimal::<PhysicalI16, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int32, &IntToDecimal::<PhysicalI32, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int64, &IntToDecimal::<PhysicalI64, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int128, &IntToDecimal::<PhysicalI128, Decimal128Type>::new(), CastRule::Explicit),
        // UInt_ -> Decimal128
        RawCastFunction::new(DataTypeId::UInt8, &IntToDecimal::<PhysicalU8, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt16, &IntToDecimal::<PhysicalU16, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt32, &IntToDecimal::<PhysicalU32, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt64, &IntToDecimal::<PhysicalU64, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt128, &IntToDecimal::<PhysicalU128, Decimal128Type>::new(), CastRule::Explicit),
        // Float_ -> Decimal128
        RawCastFunction::new(DataTypeId::Float16, &FloatToDecimal::<PhysicalF16, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        RawCastFunction::new(DataTypeId::Float32, &FloatToDecimal::<PhysicalF32, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        RawCastFunction::new(DataTypeId::Float64, &FloatToDecimal::<PhysicalF64, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        // Decimal_ -> Decimal128 (rescale)
        RawCastFunction::new(DataTypeId::Decimal64, &DecimalToDecimal::<Decimal64Type, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
        RawCastFunction::new(DataTypeId::Decimal128, &DecimalToDecimal::<Decimal128Type, Decimal128Type>::new(), TO_DECIMAL128_CAST_RULE),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct IntToDecimal<S, D> {
    _s: PhantomData<S>,
    _d: PhantomData<D>,
}

#[derive(Debug)]
pub struct IntToDecimalState<P> {
    precision: u8,
    scale: i8,
    scale_amount: P,
}

impl<S, D> IntToDecimal<S, D> {
    pub const fn new() -> Self {
        IntToDecimal {
            _s: PhantomData,
            _d: PhantomData,
        }
    }
}

impl<S, D> CastFunction for IntToDecimal<S, D>
where
    S: ScalarStorage,
    D: DecimalType,
    S::StorageType: PrimInt,
{
    type State = IntToDecimalState<D::Primitive>;

    fn bind(&self, _src: &DataType, target: &DataType) -> Result<Self::State> {
        let decimal_meta = target.try_get_decimal_type_meta()?;
        let scale = decimal_meta.scale;
        let precision = decimal_meta.precision;

        let scale_amount = <D::Primitive as NumCast>::from(10.pow(scale.unsigned_abs() as u32))
            .expect("to be in range");

        Ok(IntToDecimalState {
            precision,
            scale,
            scale_amount,
        })
    }

    fn cast(
        state: &Self::State,
        mut error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        UnaryExecutor::execute::<S, D::Storage, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |&v, buf| {
                // Convert to decimal primitive.
                let v = match <D::Primitive as NumCast>::from(v) {
                    Some(v) => v,
                    None => {
                        error_state.set_error(|| DbError::new("Failed to cast int to decimal"));
                        buf.put_null();
                        return;
                    }
                };

                // Scale.
                let val = if state.scale > 0 {
                    match v.checked_mul(&state.scale_amount) {
                        Some(v) => v,
                        None => {
                            error_state.set_error(|| DbError::new("Failed to cast int to decimal"));
                            buf.put_null();
                            return;
                        }
                    }
                } else {
                    match v.checked_div(&state.scale_amount) {
                        Some(v) => v,
                        None => {
                            error_state.set_error(|| DbError::new("Failed to cast int to decimal"));
                            buf.put_null();
                            return;
                        }
                    }
                };

                if let Err(err) = D::validate_precision(val, state.precision) {
                    error_state.set_error(|| err);
                    buf.put_null();
                    return;
                }

                buf.put(&val);
            },
        )?;

        error_state.into_result()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FloatToDecimal<S, D> {
    _s: PhantomData<S>,
    _d: PhantomData<D>,
}

#[derive(Debug)]
pub struct FloatToDecimalState<F> {
    mul_scale: F,
    precision: u8,
}

impl<S, D> FloatToDecimal<S, D> {
    pub const fn new() -> Self {
        FloatToDecimal {
            _s: PhantomData,
            _d: PhantomData,
        }
    }
}

impl<S, D> CastFunction for FloatToDecimal<S, D>
where
    S: ScalarStorage,
    S::StorageType: Float,
    D: DecimalType,
{
    type State = FloatToDecimalState<S::StorageType>;

    fn bind(&self, _src: &DataType, target: &DataType) -> Result<Self::State> {
        let decimal_meta = target.try_get_decimal_type_meta()?;
        let scale = decimal_meta.scale;
        let precision = decimal_meta.precision;

        let mul_scale = <S::StorageType as NumCast>::from(10.pow(scale.unsigned_abs() as u32))
            .ok_or_else(|| DbError::new(format!("Failed to cast scale {scale} to float")))?;

        Ok(FloatToDecimalState {
            mul_scale,
            precision,
        })
    }

    fn cast(
        state: &Self::State,
        mut error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        UnaryExecutor::execute::<S, D::Storage, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |&v, buf| {
                // TODO: Properly handle negative scale.
                let scaled_value = v.mul(state.mul_scale).round();

                match <D::Primitive as NumCast>::from(scaled_value) {
                    Some(v) => {
                        if let Err(err) = D::validate_precision(v, state.precision) {
                            error_state.set_error(|| err);
                            buf.put_null();
                            return;
                        }
                        buf.put(&v)
                    }
                    None => {
                        error_state.set_error(|| DbError::new("Failed cast decimal"));
                        buf.put_null();
                    }
                }
            },
        )?;

        error_state.into_result()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Utf8ToDecimal<D> {
    _d: PhantomData<D>,
}

#[derive(Debug)]
pub struct Utf8ToDecimalState {
    precision: u8,
    scale: i8,
}

impl<D> Utf8ToDecimal<D> {
    pub const fn new() -> Self {
        Utf8ToDecimal { _d: PhantomData }
    }
}

impl<D> CastFunction for Utf8ToDecimal<D>
where
    D: DecimalType,
{
    type State = Utf8ToDecimalState;

    fn bind(&self, _src: &DataType, target: &DataType) -> Result<Self::State> {
        let meta = target.try_get_decimal_type_meta()?;
        Ok(Utf8ToDecimalState {
            precision: meta.precision,
            scale: meta.scale,
        })
    }

    fn cast(
        state: &Self::State,
        mut error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        // TODO: Temp until... not sure. Is this actually reasonable?
        let id = out.datatype.datatype_id();
        let mut parser = DecimalParser::<D::Primitive>::new(state.precision, state.scale);

        UnaryExecutor::execute::<PhysicalUtf8, D::Storage, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |v, buf| match parser.parse(v) {
                Some(v) => buf.put(&v),
                None => {
                    error_state
                        .set_error(|| DbError::new(format!("Failed to parse '{v}' into {id}")));
                    buf.put_null();
                }
            },
        )?;

        error_state.into_result()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecimalToDecimal<D1, D2> {
    _d1: PhantomData<D1>,
    _d2: PhantomData<D2>,
}

#[derive(Debug)]
pub struct DecimalToDecimalState<P: DecimalPrimitive> {
    scale_diff: i8,
    scale_amount: P,
    /// Only used when downscaling (reducing precision)
    rounding_addition: P,
}

impl<D1, D2> DecimalToDecimal<D1, D2> {
    pub const fn new() -> Self {
        DecimalToDecimal {
            _d1: PhantomData,
            _d2: PhantomData,
        }
    }
}

impl<D1, D2> CastFunction for DecimalToDecimal<D1, D2>
where
    D1: DecimalType,
    D2: DecimalType,
{
    type State = DecimalToDecimalState<D2::Primitive>;

    fn bind(&self, src: &DataType, target: &DataType) -> Result<Self::State> {
        let target_meta = target.try_get_decimal_type_meta()?;
        let src_meta = src.try_get_decimal_type_meta()?;

        let scale_diff = src_meta.scale - target_meta.scale;
        let scale_amount = <D2::Primitive as NumCast>::from(
            10.pow((src_meta.scale - target_meta.scale).unsigned_abs() as u32),
        )
        .expect("to be in range");

        // Only used when downscaling (reducing precision)
        let rounding_addition = if scale_diff > 0 {
            scale_amount / <D2::Primitive as NumCast>::from(2).unwrap()
        } else {
            D2::Primitive::ZERO
        };

        Ok(DecimalToDecimalState {
            scale_diff,
            scale_amount,
            rounding_addition,
        })
    }

    fn cast(
        state: &Self::State,
        mut error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        UnaryExecutor::execute::<D1::Storage, D2::Storage, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |&v, buf| {
                // Convert to decimal primitive.
                let v = match <D2::Primitive as NumCast>::from(v) {
                    Some(v) => v,
                    None => {
                        error_state.set_error(|| DbError::new("Failed cast decimal"));
                        buf.put_null();
                        return;
                    }
                };

                // TODO: This if should be moved out this execute function.
                match state.scale_diff.cmp(&0) {
                    Ordering::Less => {
                        // Upscale
                        match v.checked_mul(&state.scale_amount) {
                            Some(v) => buf.put(&v),
                            None => {
                                error_state.set_error(|| DbError::new("Failed cast decimal"));
                                buf.put_null();
                            }
                        }
                    }
                    Ordering::Greater => {
                        // Downscale with rounding
                        let adjustment = if v >= D2::Primitive::ZERO {
                            state.rounding_addition
                        } else {
                            state.rounding_addition.neg()
                        };
                        match v
                            .checked_add(&adjustment)
                            .and_then(|v| v.checked_div(&state.scale_amount))
                        {
                            Some(v) => buf.put(&v),
                            None => {
                                error_state.set_error(|| DbError::new("Failed cast decimal"));
                                buf.put_null();
                            }
                        }
                    }
                    Ordering::Equal => {
                        // No change.
                        buf.put(&v)
                    }
                }
            },
        )?;

        error_state.into_result()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DecimalTypeMeta;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::functions::cast::behavior::CastFailBehavior;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn cast_decimal64_to_different_prec() {
        let mut arr = Array::try_from_iter([13000_i64]).unwrap();
        // '[1.3000]'
        arr.datatype = DataType::Decimal64(DecimalTypeMeta::new(6, 4));

        let mut out = Array::new(
            &NopBufferManager,
            DataType::Decimal64(DecimalTypeMeta::new(8, 4)),
            1,
        )
        .unwrap();

        let cast = DecimalToDecimal::<Decimal64Type, Decimal64Type>::new();
        let state = cast.bind(&arr.datatype, &out.datatype).unwrap();
        let error_state = CastFailBehavior::Error.new_state();
        DecimalToDecimal::<Decimal64Type, Decimal64Type>::cast(
            &state,
            error_state,
            &arr,
            [0],
            &mut out,
        )
        .unwrap();

        let v = PhysicalI64::get_addressable(&out.data).unwrap().slice;
        // No change.
        assert_eq!(13000, v[0]);
    }

    #[test]
    fn cast_decimal64_to_different_scale() {
        let mut arr = Array::try_from_iter([13000_i64]).unwrap();
        // '[1.3000]'
        arr.datatype = DataType::Decimal64(DecimalTypeMeta::new(6, 4));

        let mut out = Array::new(
            &NopBufferManager,
            DataType::Decimal64(DecimalTypeMeta::new(6, 2)),
            1,
        )
        .unwrap();

        let cast = DecimalToDecimal::<Decimal64Type, Decimal64Type>::new();
        let state = cast.bind(&arr.datatype, &out.datatype).unwrap();
        let error_state = CastFailBehavior::Error.new_state();
        DecimalToDecimal::<Decimal64Type, Decimal64Type>::cast(
            &state,
            error_state,
            &arr,
            [0],
            &mut out,
        )
        .unwrap();

        let v = PhysicalI64::get_addressable(&out.data).unwrap().slice;
        // Truncate right-most zeros
        assert_eq!(130, v[0]);
    }

    #[test]
    fn cast_decimal64_reduce_scale_round() {
        let mut arr =
            Array::try_from_iter([13100_i64, 13600, 13501, -13100, -13600, -13499]).unwrap();
        // '[1.3100, 1.3600, 1.3501, -1.3100, -1.3600, -1.3499]'
        arr.datatype = DataType::Decimal64(DecimalTypeMeta::new(6, 4));

        // Scale down to single decimal place.
        let mut out = Array::new(
            &NopBufferManager,
            DataType::Decimal64(DecimalTypeMeta::new(6, 1)),
            6,
        )
        .unwrap();

        let cast = DecimalToDecimal::<Decimal64Type, Decimal64Type>::new();
        let state = cast.bind(&arr.datatype, &out.datatype).unwrap();
        let error_state = CastFailBehavior::Error.new_state();
        DecimalToDecimal::<Decimal64Type, Decimal64Type>::cast(
            &state,
            error_state,
            &arr,
            0..6,
            &mut out,
        )
        .unwrap();

        let v = PhysicalI64::get_addressable(&out.data).unwrap().slice;
        assert_eq!(13, v[0]); // Round down (1.3100 -> 1.3)
        assert_eq!(14, v[1]); // Round up (1.3600 -> 1.4)
        assert_eq!(14, v[2]); // Round up (1.3501 -> 1.4)
        assert_eq!(-13, v[3]); // Round up (-1.3100 -> -1.3)
        assert_eq!(-14, v[4]); // Round up (-1.3600 -> -1.4)
        assert_eq!(-13, v[5]); // Round up (-1.3499 -> -1.3)
    }
}
