use glaredb_error::{DbError, Result};

use super::null::NullToAnything;
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalI32, PhysicalUtf8};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::functions::cast::behavior::CastErrorState;
use crate::functions::cast::parse::{Date32Parser, Parser};
use crate::functions::cast::{CastFunction, CastFunctionSet, RawCastFunction, TO_DATE32_CAST_RULE};
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_TO_DATE32: CastFunctionSet = CastFunctionSet {
    name: "to_date32",
    target: DataTypeId::Date32,
    functions: &[
        // Null -> Date32
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_DATE32_CAST_RULE),
        // Utf8 -> Date32
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToDate32, TO_DATE32_CAST_RULE),
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
