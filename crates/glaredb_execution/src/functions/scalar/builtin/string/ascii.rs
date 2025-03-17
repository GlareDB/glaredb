use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalI32, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_ASCII: ScalarFunctionSet = ScalarFunctionSet {
    name: "ascii",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Get the ascii code of the first character of the argument.",
        arguments: &["string"],
        example: Some(Example {
            example: "ascii('h')",
            output: "104",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Int32),
        &Ascii,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ascii;

impl ScalarFunction for Ascii {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Int32,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalI32, _>(
            input,
            sel,
            OutBuffer::from_array(output)?,
            |v, buf| {
                let v = v.chars().next().map(|c| c as i32).unwrap_or(0);
                buf.put(&v)
            },
        )
    }
}
