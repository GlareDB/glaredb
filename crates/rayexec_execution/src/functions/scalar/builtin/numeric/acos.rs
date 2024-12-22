use num_traits::Float;
use rayexec_bullet::array::{ArrayData, ArrayOld};
use rayexec_bullet::datatype::DataTypeOld;
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::PhysicalStorageOld;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};

pub type Acos = UnaryInputNumericScalar<AcosOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcosOp;

impl UnaryInputNumericOperation for AcosOp {
    const NAME: &'static str = "acos";
    const DESCRIPTION: &'static str = "Compute the arccosine of value";

    fn execute_float<'a, S>(input: &'a ArrayOld, ret: DataTypeOld) -> Result<ArrayOld>
    where
        S: PhysicalStorageOld,
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
