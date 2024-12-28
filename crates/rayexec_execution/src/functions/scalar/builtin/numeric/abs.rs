use num_traits::Float;
use rayexec_error::Result;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::{Array2, ArrayData2};
use crate::arrays::datatype::DataType;
use crate::arrays::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use crate::arrays::executor::physical_type::PhysicalStorage2;
use crate::arrays::executor::scalar::UnaryExecutor2;
use crate::arrays::storage::PrimitiveStorage;

pub type Abs = UnaryInputNumericScalar<AbsOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbsOp;

impl UnaryInputNumericOperation for AbsOp {
    const NAME: &'static str = "abs";
    const DESCRIPTION: &'static str = "Compute the absolute value of a number";

    fn execute_float<'a, S>(input: &'a Array2, ret: DataType) -> Result<Array2>
    where
        S: PhysicalStorage2,
        S::Type<'a>: Float + Default,
        ArrayData2: From<PrimitiveStorage<S::Type<'a>>>,
    {
        let builder = ArrayBuilder {
            datatype: ret,
            buffer: PrimitiveBuffer::with_len(input.logical_len()),
        };
        UnaryExecutor2::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.abs()))
    }
}
