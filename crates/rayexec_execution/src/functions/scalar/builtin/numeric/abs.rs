use stdutil::iter::IntoExactSizeIterator;
use num_traits::Float;
use rayexec_error::Result;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::exp::Array;
use crate::arrays::array::{Array2, ArrayData2};
use crate::arrays::buffer::physical_type::MutablePhysicalStorage;
use crate::arrays::datatype::DataType;
use crate::arrays::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use crate::arrays::executor::physical_type::PhysicalStorage2;
use crate::arrays::executor::scalar::UnaryExecutor2;
use crate::arrays::executor_exp::scalar::unary::UnaryExecutor;
use crate::arrays::executor_exp::OutBuffer;
use crate::arrays::storage::PrimitiveStorage;

pub type Abs = UnaryInputNumericScalar<AbsOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbsOp;

impl UnaryInputNumericOperation for AbsOp {
    const NAME: &'static str = "abs";
    const DESCRIPTION: &'static str = "Compute the absolute value of a number";

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
            |&v, buf| buf.put(&v.abs()),
        )
    }
}
