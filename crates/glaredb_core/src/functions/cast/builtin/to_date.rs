use glaredb_error::{DbError, Result};

use super::null::NullToAnything;
use super::to_primitive::PrimToPrim;
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    PhysicalI8, PhysicalI16, PhysicalI32, PhysicalI64, PhysicalI128, PhysicalU8, PhysicalU16,
    PhysicalU32, PhysicalU64, PhysicalU128, PhysicalUtf8,
};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::functions::cast::behavior::CastErrorState;
use crate::functions::cast::parse::{Date32Parser, Parser};
use crate::functions::cast::{
    CastFlatten, CastFunction, CastFunctionSet, CastRule, RawCastFunction, TO_DATE32_CAST_RULE,
};
use crate::util::iter::IntoExactSizeIterator;

/// Function set for casting to Date32.
///
/// Date32 is internally represented as a signed int indicating days since Unix
/// epoch. The integer casts just casts to number of days.
pub const FUNCTION_SET_TO_DATE32: CastFunctionSet = CastFunctionSet {
    name: "to_date32",
    target: DataTypeId::Date32,
    #[rustfmt::skip]
    functions: &[
        // Null -> Date32
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_DATE32_CAST_RULE, CastFlatten::Safe),
        // Int_ -> Date32
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalI32>::new(), CastRule::Explicit, CastFlatten::Unsafe),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalI32>::new(), CastRule::Explicit, CastFlatten::Unsafe),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalI32>::new(), CastRule::Explicit, CastFlatten::Unsafe),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalI32>::new(), CastRule::Explicit, CastFlatten::Unsafe),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalI32>::new(), CastRule::Explicit, CastFlatten::Unsafe),
        // UInt_ -> Date32
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalI32>::new(), CastRule::Explicit, CastFlatten::Unsafe),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalI32>::new(), CastRule::Explicit, CastFlatten::Unsafe),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalI32>::new(), CastRule::Explicit, CastFlatten::Unsafe),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalI32>::new(), CastRule::Explicit, CastFlatten::Unsafe),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalI32>::new(), CastRule::Explicit, CastFlatten::Unsafe),
        // Utf8 -> Date32
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToDate32, TO_DATE32_CAST_RULE, CastFlatten::Unsafe),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Utf8ToDate32;

impl CastFunction for Utf8ToDate32 {
    type State = ();

    fn bind(&self, _src: &DataType, _target: &DataType) -> Result<Self::State> {
        Ok(())
    }

    fn cast(
        _: &Self::State,
        mut error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        UnaryExecutor::execute::<PhysicalUtf8, PhysicalI32, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |v, buf| match Date32Parser.parse(v) {
                Some(v) => buf.put(&v),
                None => {
                    error_state
                        .set_error(|| DbError::new(format!("Failed to parse '{v}' into date")));
                    buf.put_null();
                }
            },
        )?;

        error_state.into_result()
    }
}
