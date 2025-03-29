use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::PhysicalUtf8;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_REVERSE: ScalarFunctionSet = ScalarFunctionSet {
    name: "reverse",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Reverse the input string.",
        arguments: &["string"],
        example: Some(Example {
            example: "reverse('hello')",
            output: "olleh",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Utf8),
        &Reverse,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reverse;

impl ScalarFunction for Reverse {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Utf8,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        let mut s_buf = String::new();

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            input,
            sel,
            OutBuffer::from_array(output)?,
            |v, buf| {
                s_buf.clear();
                s_buf.extend(v.chars().rev());
                buf.put(&s_buf)
            },
        )
    }
}
