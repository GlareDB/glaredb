use glaredb_error::{DbError, Result};

use super::null::NullToAnything;
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalBool, PhysicalUtf8};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::functions::cast::behavior::CastErrorState;
use crate::functions::cast::parse::{BoolParser, Parser};
use crate::functions::cast::{CastFunction, CastFunctionSet, RawCastFunction, TO_BOOL_CAST_RULE};
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_TO_BOOLEAN: CastFunctionSet = CastFunctionSet {
    name: "to_boolean",
    target: DataTypeId::Boolean,
    functions: &[
        // Null -> Bool
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_BOOL_CAST_RULE),
        // Utf8 -> Bool
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToBool, TO_BOOL_CAST_RULE),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Utf8ToBool;

impl CastFunction for Utf8ToBool {
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
        UnaryExecutor::execute::<PhysicalUtf8, PhysicalBool, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |v, buf| match BoolParser.parse(v) {
                Some(v) => buf.put(&v),
                None => {
                    error_state
                        .set_error(|| DbError::new(format!("Failed to parse '{v}' into boolean")));
                    buf.put_null();
                }
            },
        )?;

        error_state.into_result()
    }
}
