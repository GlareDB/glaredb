use num_traits::Float;
use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::physical_type::MutableScalarStorage;
use crate::arrays::array::Array;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;

pub type Asin = UnaryInputNumericScalar<AsinOp>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsinOp;

impl UnaryInputNumericOperation for AsinOp {
    const NAME: &'static str = "asin";
    const DESCRIPTION: &'static str = "Compute the arcsine of value";

    fn execute_float<S>(
        input: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        output: &mut Array,
    ) -> Result<()>
    where
        S: MutableScalarStorage,
        S::StorageType: Float,
    {
        UnaryExecutor::execute::<S, S, _>(
            input,
            selection,
            OutBuffer::from_array(output)?,
            |&v, buf| buf.put(&v.asin()),
        )
    }
}
