use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::PhysicalF64;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, FunctionVolatility, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_RANDOM: ScalarFunctionSet = ScalarFunctionSet {
    name: "random",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Return a random float.",
        arguments: &[],
        example: Some(Example {
            example: "random()",
            output: "0.7268028627434533 -- (varies, returns random value between 0 and 1)",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[], DataTypeId::Float64),
        &Random,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Random;

impl ScalarFunction for Random {
    const VOLATILITY: FunctionVolatility = FunctionVolatility::Volatile;

    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::float64(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        UnaryExecutor::execute_in_place::<PhysicalF64, _>(output, sel, |v| {
            *v = rand::random::<f64>()
        })
    }
}
