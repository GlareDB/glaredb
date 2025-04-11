use glaredb_error::Result;
use num_traits::Float;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::RawScalarFunction;
use crate::functions::scalar::binary::{BinaryInputNumericOperation, BinaryInputNumericScalar};
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_ATAN2: ScalarFunctionSet = ScalarFunctionSet {
    name: "atan2",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Compute the arctangent of y/x.",
        arguments: &["y", "x"],
        example: None,
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float16, DataTypeId::Float16], DataTypeId::Float16),
            &BinaryInputNumericScalar::<PhysicalF16, Atan2Op>::new(&DataType::Float16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float32, DataTypeId::Float32], DataTypeId::Float32),
            &BinaryInputNumericScalar::<PhysicalF32, Atan2Op>::new(&DataType::Float32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float64, DataTypeId::Float64], DataTypeId::Float64),
            &BinaryInputNumericScalar::<PhysicalF64, Atan2Op>::new(&DataType::Float64),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Atan2Op;

impl BinaryInputNumericOperation for Atan2Op {
    fn execute_float<S>(
        left: &Array,
        right: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        output: &mut Array,
    ) -> Result<()>
    where
        S: MutableScalarStorage,
        S::StorageType: Float,
    {
        BinaryExecutor::execute::<S, S, S, _>(
            left,
            right,
            selection,
            OutBuffer::from_array(output)?,
            |&y, &x, buf| buf.put(&y.atan2(x)),
        )
    }
}
