use glaredb_error::Result;

use super::null::NullToAnything;
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalBinary, PhysicalUtf8};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::functions::cast::behavior::CastErrorState;
use crate::functions::cast::{
    CastFlatten, CastFunction, CastFunctionSet, CastRule, RawCastFunction,
};
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_TO_BINARY: CastFunctionSet = CastFunctionSet {
    name: "to_binary",
    target: DataTypeId::Binary,
    #[rustfmt::skip]
    functions: &[
        // Null
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, CastRule::Explicit, CastFlatten::Safe),
        // String
        RawCastFunction::new(DataTypeId::Utf8, &StringToBinary, CastRule::Explicit, CastFlatten::Unsafe),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct StringToBinary;

impl CastFunction for StringToBinary {
    type State = ();

    fn bind(&self, _src: &DataType, _target: &DataType) -> Result<Self::State> {
        Ok(())
    }

    fn cast(
        _state: &Self::State,
        _error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        // TODO: Don't copy
        UnaryExecutor::execute::<PhysicalUtf8, PhysicalBinary, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |v, buf| {
                buf.put(v.as_bytes());
            },
        )
    }
}
