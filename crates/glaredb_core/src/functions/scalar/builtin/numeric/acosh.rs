use glaredb_error::Result;
use num_traits::Float;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalF64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::RawScalarFunction;
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_ACOSH: ScalarFunctionSet = ScalarFunctionSet {
    name: "acosh",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Compute the inverse hyperbolic cosine of value.",
        arguments: &["float"],
        example: Some(Example {
            example: "acosh(1)",
            output: "0",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
        &UnaryInputNumericScalar::<PhysicalF64, AcoshOp>::new(DataType::FLOAT64),
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcoshOp;

impl UnaryInputNumericOperation for AcoshOp {
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
            |&v, buf| buf.put(&v.acosh()),
        )
    }
}
