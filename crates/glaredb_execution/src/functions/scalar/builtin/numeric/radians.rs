use num_traits::Float;
use glaredb_error::Result;

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
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_RADIANS: ScalarFunctionSet = ScalarFunctionSet {
    name: "radians",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Numeric,
        description: "Convert degrees to radians",
        arguments: &["float"],
        example: None,
    }),
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &UnaryInputNumericScalar::<PhysicalF16, RadiansOp>::new(&DataType::Float16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &UnaryInputNumericScalar::<PhysicalF32, RadiansOp>::new(&DataType::Float32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &UnaryInputNumericScalar::<PhysicalF64, RadiansOp>::new(&DataType::Float64),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RadiansOp;

impl UnaryInputNumericOperation for RadiansOp {
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
            |&v, buf| buf.put(&v.to_radians()),
        )
    }
}
