use num_traits::Float;
use crate::arrays::array::{Array, ArrayData};
use crate::arrays::datatype::DataType;
use crate::arrays::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use crate::arrays::executor::physical_type::PhysicalStorage;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::storage::PrimitiveStorage;
use rayexec_error::Result;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};

pub type Cbrt = UnaryInputNumericScalar<CbrtOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CbrtOp;

impl UnaryInputNumericOperation for CbrtOp {
    const NAME: &'static str = "cbrt";
    const DESCRIPTION: &'static str = "Compute the cube root of value";

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
        UnaryExecutor::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.cbrt()))
    }
}
