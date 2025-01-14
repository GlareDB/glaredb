use num_traits::Float;
use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::physical_type::MutablePhysicalStorage;
use crate::arrays::array::Array;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;

pub type Log = UnaryInputNumericScalar<LogOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogOp;

impl UnaryInputNumericOperation for LogOp {
    const NAME: &'static str = "log";
    const DESCRIPTION: &'static str = "Compute base-10 log of value";

    fn execute_float<S>(
        input: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        output: &mut Array,
    ) -> Result<()>
    where
        S: MutablePhysicalStorage,
        S::StorageType: Float,
    {
        UnaryExecutor::execute::<S, S, _>(
            input,
            selection,
            OutBuffer::from_array(output)?,
            |&v, buf| buf.put(&v.log10()),
        )
    }
}

pub type Log2 = UnaryInputNumericScalar<LogOp2>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogOp2;

impl UnaryInputNumericOperation for LogOp2 {
    const NAME: &'static str = "log2";
    const DESCRIPTION: &'static str = "Compute base-2 log of value";

    fn execute_float<S>(
        input: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        output: &mut Array,
    ) -> Result<()>
    where
        S: MutablePhysicalStorage,
        S::StorageType: Float,
    {
        UnaryExecutor::execute::<S, S, _>(
            input,
            selection,
            OutBuffer::from_array(output)?,
            |&v, buf| buf.put(&v.log2()),
        )
    }
}
