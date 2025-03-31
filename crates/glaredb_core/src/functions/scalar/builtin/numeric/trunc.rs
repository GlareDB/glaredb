use glaredb_error::Result;
use num_traits::{Float, Zero};

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

pub const FUNCTION_SET_TRUNC: ScalarFunctionSet = ScalarFunctionSet {
    name: "trunc",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Truncate number towards zero",
        arguments: &["float"],
        example: Some(Example {
            example: "trunc(-1.9)",
            output: "-1",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &UnaryInputNumericScalar::<PhysicalF16, TruncOp>::new(&DataType::Float16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &UnaryInputNumericScalar::<PhysicalF32, TruncOp>::new(&DataType::Float32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &UnaryInputNumericScalar::<PhysicalF64, TruncOp>::new(&DataType::Float64),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TruncOp;

impl UnaryInputNumericOperation for TruncOp {
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
                let truncated = if v < S::StorageType::zero() {
                    v.ceil()
                } else {
                    v.floor()
                };
                buf.put(&truncated)
            },
        )
    }
}
