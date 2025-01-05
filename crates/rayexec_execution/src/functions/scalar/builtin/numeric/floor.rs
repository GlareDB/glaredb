use stdutil::iter::IntoExactSizeIterator;
use num_traits::Float;
use rayexec_error::Result;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::exp::Array;
use crate::arrays::buffer::physical_type::MutablePhysicalStorage;
use crate::arrays::executor_exp::scalar::unary::UnaryExecutor;
use crate::arrays::executor_exp::OutBuffer;

pub type Floor = UnaryInputNumericScalar<FloorOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorOp;

impl UnaryInputNumericOperation for FloorOp {
    const NAME: &'static str = "floor";
    const DESCRIPTION: &'static str = "Round number down";

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
            |&v, buf| buf.put(&v.floor()),
        )
    }
}
