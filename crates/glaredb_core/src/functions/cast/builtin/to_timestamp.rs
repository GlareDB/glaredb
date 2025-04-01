use glaredb_error::Result;

use super::null::NullToAnything;
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalI64, PhysicalUtf8};
use crate::arrays::datatype::{DataType, DataTypeId, TimeUnit};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::functions::cast::behavior::CastErrorState;
use crate::functions::cast::parse::{Parser, TimestampParser};
use crate::functions::cast::{
    CastFunction,
    CastFunctionSet,
    RawCastFunction,
    TO_TIMESTAMP_CAST_RULE,
};
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_TO_TIMESTAMP: CastFunctionSet = CastFunctionSet {
    name: "to_timestamp",
    target: DataTypeId::Timestamp,
    functions: &[
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_TIMESTAMP_CAST_RULE),
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToTimestamp, TO_TIMESTAMP_CAST_RULE),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Utf8ToTimestamp;

impl CastFunction for Utf8ToTimestamp {
    type State = TimeUnit;

    fn bind(&self, _src: &DataType, target: &DataType) -> Result<Self::State> {
        match target {
            DataType::Timestamp(meta) => Ok(meta.unit),
            _ => Ok(TimeUnit::Microsecond), // Default to microseconds
        }
    }

    fn cast(
        unit: &Self::State,
        error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        let mut parser = TimestampParser { unit: *unit };

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalI64, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |v, buf| match parser.parse(v) {
                Some(v) => buf.put(&v),
                None => {
                    buf.put_null();
                }
            },
        )?;

        error_state.into_result()
    }
}
