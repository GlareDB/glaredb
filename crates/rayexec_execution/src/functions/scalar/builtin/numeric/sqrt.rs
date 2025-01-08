use num_traits::Float;
use rayexec_error::Result;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::physical_type::PhysicalStorage;
use crate::arrays::array::{Array, ArrayData};
use crate::arrays::datatype::DataType;
use crate::arrays::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::storage::PrimitiveStorage;

pub type Sqrt = UnaryInputNumericScalar<SqrtOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqrtOp;

impl UnaryInputNumericOperation for SqrtOp {
    const NAME: &'static str = "sqrt";
    const DESCRIPTION: &'static str = "Compute the square root of value";

    fn execute_float<'a, S>(input: &'a Array, ret: DataType) -> Result<Array>
    where
        S: PhysicalStorage,
        S::Type<'a>: Float + Default,
        ArrayData: From<PrimitiveStorage<S::Type<'a>>>,
    {
        let builder = ArrayBuilder {
            datatype: ret,
            buffer: PrimitiveBuffer::with_len(input.logical_len()),
        };
        UnaryExecutor::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.sqrt()))
    }
}
