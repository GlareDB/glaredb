use glaredb_error::Result;
use num_traits::Float;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::RawScalarFunction;
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_DEGREES: ScalarFunctionSet = ScalarFunctionSet {
    name: "degrees",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Convert radians to degrees.",
        arguments: &["float"],
        example: Some(Example {
            example: "degrees(3.141592653589793)",
            output: "180",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &UnaryInputNumericScalar::<PhysicalF16, DegreesOp>::new(DataType::FLOAT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &UnaryInputNumericScalar::<PhysicalF32, DegreesOp>::new(DataType::FLOAT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &UnaryInputNumericScalar::<PhysicalF64, DegreesOp>::new(DataType::FLOAT64),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DegreesOp;

impl UnaryInputNumericOperation for DegreesOp {
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
            |&v, buf| buf.put(&v.to_degrees()),
        )
    }
}
