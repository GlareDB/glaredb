use num_traits::Float;
use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
};
use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::RawScalarFunction;
use crate::functions::Signature;

pub const FUNCTION_SET_TAN: ScalarFunctionSet = ScalarFunctionSet {
    name: "tan",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Numeric,
        description: "Compute the tangent of value",
        arguments: &["float"],
        example: None,
    }),
    functions: &[
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &UnaryInputNumericScalar::<PhysicalF16, TanOp>::new(&DataType::Float16),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &UnaryInputNumericScalar::<PhysicalF32, TanOp>::new(&DataType::Float32),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &UnaryInputNumericScalar::<PhysicalF64, TanOp>::new(&DataType::Float64),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TanOp;

impl UnaryInputNumericOperation for TanOp {
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
            |&v, buf| buf.put(&v.tan()),
        )
    }
}
