use iterutil::IntoExactSizeIterator;
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

pub type Cos = UnaryInputNumericScalar<CosOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CosOp;

impl UnaryInputNumericOperation for CosOp {
    const NAME: &'static str = "cos";
    const DESCRIPTION: &'static str = "Compute the cosine of value";

    fn execute_float2<'a, S>(input: &'a Array2, ret: DataType) -> Result<Array2>
    where
        S: PhysicalStorage2,
        S::Type<'a>: Float + Default,
        ArrayData2: From<PrimitiveStorage<S::Type<'a>>>,
    {
        let builder = ArrayBuilder {
            datatype: ret,
            buffer: PrimitiveBuffer::with_len(input.logical_len()),
        };
        UnaryExecutor2::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.cos()))
    }

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
            |&v, buf| buf.put(&v.cos()),
        )
    }
}
