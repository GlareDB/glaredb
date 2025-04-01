use std::fmt::{Display, Write};
use std::marker::PhantomData;

use glaredb_error::{DbError, Result};

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
    PhysicalInterval,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
    PhysicalUtf8,
    ScalarStorage,
};
use crate::arrays::datatype::{DataType, DataTypeId, TimeUnit};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::scalar::decimal::{
    Decimal64Type,
    Decimal128Type,
    DecimalPrimitive,
    DecimalType,
};
use crate::functions::cast::behavior::CastErrorState;
use crate::functions::cast::format::{
    DecimalFormatter,
    Formatter,
    TimestampMicrosecondsFormatter,
    TimestampMillisecondsFormatter,
    TimestampNanosecondsFormatter,
    TimestampSecondsFormatter,
};
use crate::functions::cast::{CastFunction, CastFunctionSet, RawCastFunction, TO_STRING_CAST_RULE};
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_TO_STRING: CastFunctionSet = CastFunctionSet {
    name: "to_string",
    target: DataTypeId::Utf8,
    functions: &[
        // Null
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_STRING_CAST_RULE),
        // Primitives
        RawCastFunction::new(
            DataTypeId::Int8,
            &PrimToString::<PhysicalI8>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::Int16,
            &PrimToString::<PhysicalI16>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::Int32,
            &PrimToString::<PhysicalI32>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::Int64,
            &PrimToString::<PhysicalI64>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::Int128,
            &PrimToString::<PhysicalI128>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::UInt8,
            &PrimToString::<PhysicalU8>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::UInt16,
            &PrimToString::<PhysicalU16>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::UInt32,
            &PrimToString::<PhysicalU32>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::UInt64,
            &PrimToString::<PhysicalU64>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::UInt128,
            &PrimToString::<PhysicalU128>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::Float16,
            &PrimToString::<PhysicalF16>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::Float32,
            &PrimToString::<PhysicalF32>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::Float64,
            &PrimToString::<PhysicalF64>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::Interval,
            &PrimToString::<PhysicalInterval>::new(),
            TO_STRING_CAST_RULE,
        ),
        // Decimals
        RawCastFunction::new(
            DataTypeId::Decimal64,
            &DecimalToString::<Decimal64Type>::new(),
            TO_STRING_CAST_RULE,
        ),
        RawCastFunction::new(
            DataTypeId::Decimal128,
            &DecimalToString::<Decimal128Type>::new(),
            TO_STRING_CAST_RULE,
        ),
        // Timestamp
        RawCastFunction::new(
            DataTypeId::Timestamp,
            &TimestampToString,
            TO_STRING_CAST_RULE,
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct PrimToString<S> {
    _s: PhantomData<S>,
}

impl<S> PrimToString<S> {
    pub const fn new() -> Self {
        PrimToString { _s: PhantomData }
    }
}

impl<S> CastFunction for PrimToString<S>
where
    S: ScalarStorage,
    S::StorageType: Display,
{
    type State = ();

    fn bind(&self, _src: &DataType, _target: &DataType) -> Result<Self::State> {
        Ok(())
    }

    fn cast(
        _state: &Self::State,
        mut error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        let mut s_buf = String::new();

        UnaryExecutor::execute::<S, PhysicalUtf8, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |v, buf| {
                s_buf.clear();
                match write!(s_buf, "{}", v) {
                    Ok(_) => buf.put(s_buf.as_str()),
                    Err(_) => {
                        error_state.set_error(|| DbError::new("Failed to cast to utf8"));
                        buf.put_null();
                    }
                }
            },
        )?;

        error_state.into_result()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecimalToString<D> {
    _d: PhantomData<D>,
}

#[derive(Debug)]
pub struct DecimalToStringState<P: DecimalPrimitive> {
    formatter: DecimalFormatter<P>,
}

impl<D> DecimalToString<D>
where
    D: DecimalType,
{
    pub const fn new() -> Self {
        DecimalToString { _d: PhantomData }
    }
}

impl<D> CastFunction for DecimalToString<D>
where
    D: DecimalType,
{
    type State = DecimalToStringState<D::Primitive>;

    fn bind(&self, src: &DataType, _target: &DataType) -> Result<Self::State> {
        let meta = src.try_get_decimal_type_meta()?;
        let formatter = DecimalFormatter::new(meta.precision, meta.scale);

        Ok(DecimalToStringState { formatter })
    }

    fn cast(
        state: &Self::State,
        error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        cast_with_formatter::<_, D::Storage>(error_state, &state.formatter, src, sel, out)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TimestampToString;

#[derive(Debug)]
pub struct TimestampToStringState {
    unit: TimeUnit,
}

impl CastFunction for TimestampToString {
    type State = TimestampToStringState;

    fn bind(&self, src: &DataType, _target: &DataType) -> Result<Self::State> {
        let m = src.try_get_timestamp_type_meta()?;
        Ok(TimestampToStringState { unit: m.unit })
    }

    fn cast(
        state: &Self::State,
        error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        match state.unit {
            TimeUnit::Second => cast_with_formatter::<_, PhysicalI64>(
                error_state,
                &TimestampSecondsFormatter::default(),
                src,
                sel,
                out,
            ),
            TimeUnit::Millisecond => cast_with_formatter::<_, PhysicalI64>(
                error_state,
                &TimestampMillisecondsFormatter::default(),
                src,
                sel,
                out,
            ),
            TimeUnit::Microsecond => cast_with_formatter::<_, PhysicalI64>(
                error_state,
                &TimestampMicrosecondsFormatter::default(),
                src,
                sel,
                out,
            ),
            TimeUnit::Nanosecond => cast_with_formatter::<_, PhysicalI64>(
                error_state,
                &TimestampNanosecondsFormatter::default(),
                src,
                sel,
                out,
            ),
        }
    }
}

fn cast_with_formatter<F, S>(
    mut error_state: CastErrorState,
    formatter: &F,
    src: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
) -> Result<()>
where
    F: Formatter,
    S: ScalarStorage<StorageType = F::Type>,
{
    let mut s_buf = String::new();

    UnaryExecutor::execute::<S, PhysicalUtf8, _>(
        src,
        sel,
        OutBuffer::from_array(out)?,
        |v, buf| {
            s_buf.clear();
            match formatter.write(v, &mut s_buf) {
                Ok(_) => buf.put(s_buf.as_str()),
                Err(_) => {
                    error_state.set_error(|| DbError::new("Failed to cast to utf8"));
                    buf.put_null();
                }
            }
        },
    )?;

    error_state.into_result()
}
