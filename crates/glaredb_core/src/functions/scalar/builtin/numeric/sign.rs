use glaredb_error::Result;
use num_traits::{Float, One, Zero};

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

pub const FUNCTION_SET_SIGN: ScalarFunctionSet = ScalarFunctionSet {
    name: "sign",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Sign of the argument (-1, 0, or +1).",
        arguments: &["numeric"],
        example: Some(Example {
            example: "sign(-8.4)",
            output: "-1",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &UnaryInputNumericScalar::<PhysicalF16, SignOp>::new(DataType::FLOAT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &UnaryInputNumericScalar::<PhysicalF32, SignOp>::new(DataType::FLOAT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &UnaryInputNumericScalar::<PhysicalF64, SignOp>::new(DataType::FLOAT64),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignOp;

impl UnaryInputNumericOperation for SignOp {
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
            |&v, buf| {
                let result = if v.is_nan() {
                    S::StorageType::zero()
                } else if v.is_zero() {
                    S::StorageType::zero()
                } else if v > S::StorageType::zero() {
                    S::StorageType::one()
                } else {
                    -S::StorageType::one()
                };
                buf.put(&result)
            },
        )
    }
}
