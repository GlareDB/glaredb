use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::PhysicalF64;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_PI: ScalarFunctionSet = ScalarFunctionSet {
    name: "pi",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Return the value of pi.",
        arguments: &[],
        example: Some(Example {
            example: "pi()",
            output: "3.141592653589793",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[], DataTypeId::Float64),
        &Pi,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pi;

impl ScalarFunction for Pi {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Float64,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        UnaryExecutor::execute_in_place::<PhysicalF64, _>(output, sel, |v| {
            *v = std::f64::consts::PI
        })
    }
}
