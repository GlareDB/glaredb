use glaredb_error::{DbError, Result};

use super::null::NullToAnything;
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalInterval, PhysicalUtf8};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::functions::cast::behavior::CastErrorState;
use crate::functions::cast::parse::{IntervalParser, Parser};
use crate::functions::cast::{
    CastFunction,
    CastFunctionSet,
    RawCastFunction,
    TO_INTERVAL_CAST_RULE,
};
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_TO_INTERVAL: CastFunctionSet = CastFunctionSet {
    name: "to_interval",
    target: DataTypeId::Interval,
    functions: &[
        // Null -> Interval
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_INTERVAL_CAST_RULE),
        // Utf8 -> Interval
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToInterval, TO_INTERVAL_CAST_RULE),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Utf8ToInterval;

impl CastFunction for Utf8ToInterval {
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
        let mut parser = IntervalParser::default();

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalInterval, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |v, buf| match parser.parse(v) {
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
