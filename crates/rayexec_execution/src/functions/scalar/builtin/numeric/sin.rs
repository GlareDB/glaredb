use num_traits::Float;
use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::physical_type::MutablePhysicalStorage;
use crate::arrays::array::Array;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;

pub type Sin = UnaryInputNumericScalar<SinOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinOp;

impl UnaryInputNumericOperation for SinOp {
    const NAME: &'static str = "sin";
    const DESCRIPTION: &'static str = "Compute the sin of value";

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
            |&v, buf| buf.put(&v.sin()),
        )
    }
}
