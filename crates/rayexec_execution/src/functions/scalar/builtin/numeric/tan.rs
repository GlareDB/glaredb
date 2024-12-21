use num_traits::Float;
use rayexec_bullet::array::{ArrayData, ArrayOld};
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::PhysicalStorageOld;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};

pub type Tan = UnaryInputNumericScalar<TanOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TanOp;

impl UnaryInputNumericOperation for TanOp {
    const NAME: &'static str = "tan";
    const DESCRIPTION: &'static str = "Compute the tangent of value";

    fn execute_float<'a, S>(input: &'a ArrayOld, ret: DataType) -> Result<ArrayOld>
    where
        S: PhysicalStorageOld,
        S::Type<'a>: Float + Default,
        ArrayData: From<PrimitiveStorage<S::Type<'a>>>,
    {
        let builder = ArrayBuilder {
            datatype: ret,
            buffer: PrimitiveBuffer::with_len(input.logical_len()),
        };
        UnaryExecutor::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.tan()))
    }
}
