use glaredb_error::Result;
use num_traits::{Float, One, Zero};

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalF64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::RawScalarFunction;
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_COT: ScalarFunctionSet = ScalarFunctionSet {
    name: "cot",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Compute the cotangent of value.",
        arguments: &["float"],
        example: None,
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &UnaryInputNumericScalar::<PhysicalF64, CotOp>::new(&DataType::Float64),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CotOp;

impl UnaryInputNumericOperation for CotOp {
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
                let tan_val = v.tan();
                if tan_val.is_zero() {
                    buf.put(&S::StorageType::infinity())
                } else {
                    buf.put(&(S::StorageType::one() / tan_val))
                }
            },
        )
    }
}
