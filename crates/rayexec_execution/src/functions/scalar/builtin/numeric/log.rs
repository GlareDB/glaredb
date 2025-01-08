use num_traits::Float;
use rayexec_error::Result;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::{Array, ArrayData};
use crate::arrays::datatype::DataType;
use crate::arrays::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use crate::arrays::array::physical_type::PhysicalStorage;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::storage::PrimitiveStorage;

pub type Log = UnaryInputNumericScalar<LogOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogOp;

impl UnaryInputNumericOperation for LogOp {
    const NAME: &'static str = "log";
    const DESCRIPTION: &'static str = "Compute base-10 log of value";

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
        UnaryExecutor::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.log10()))
    }
}

pub type Log2 = UnaryInputNumericScalar<LogOp2>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogOp2;

impl UnaryInputNumericOperation for LogOp2 {
    const NAME: &'static str = "log2";
    const DESCRIPTION: &'static str = "Compute base-2 log of value";

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
        UnaryExecutor::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.log2()))
    }
}
