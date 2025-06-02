use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalI64, PhysicalI128};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_FACTORIAL: ScalarFunctionSet = ScalarFunctionSet {
    name: "factorial",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Compute the factorial of an integer.",
        arguments: &["n"],
        example: Some(Example {
            example: "factorial(5)",
            output: "120",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Int64], DataTypeId::Int128),
        &Factorial,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Factorial;

impl ScalarFunction for Factorial {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::int128(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input_array = &input.arrays()[0];

        UnaryExecutor::execute::<PhysicalI64, PhysicalI128, _>(
            input_array,
            sel,
            OutBuffer::from_array(output)?,
            |&n, buf| {
                if n < 0 {
                    buf.put_null();
                    return;
                }

                if n == 0 || n == 1 {
                    buf.put(&1i128);
                    return;
                }

                let mut result = 1i128;
                for i in 2..=n {
                    match result.checked_mul(i as i128) {
                        Some(new_result) => result = new_result,
                        None => {
                            buf.put_null();
                            return;
                        }
                    }
                }

                buf.put(&result);
            },
        )
    }
}
