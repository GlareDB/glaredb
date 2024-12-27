use num_traits::Float;
use rayexec_error::Result;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::{Array2, ArrayData};
use crate::arrays::datatype::DataType;
use crate::arrays::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use crate::arrays::executor::physical_type::PhysicalStorage;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::storage::PrimitiveStorage;

pub type Acos = UnaryInputNumericScalar<AcosOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcosOp;

impl UnaryInputNumericOperation for AcosOp {
    const NAME: &'static str = "acos";
    const DESCRIPTION: &'static str = "Compute the arccosine of value";

    fn execute_float<'a, S>(input: &'a Array2, ret: DataType) -> Result<Array2>
    where
        S: PhysicalStorage,
        S::Type<'a>: Float + Default,
        ArrayData: From<PrimitiveStorage<S::Type<'a>>>,
    {
        let builder = ArrayBuilder {
            datatype: ret,
            buffer: PrimitiveBuffer::with_len(input.logical_len()),
        };
        UnaryExecutor::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.acos()))
    }
}
