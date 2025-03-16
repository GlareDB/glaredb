use glaredb_error::Result;

use crate::arrays::array::physical_type::PhysicalUtf8;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_LOWER: ScalarFunctionSet = ScalarFunctionSet {
    name: "lower",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Convert the string to lowercase.",
        arguments: &["string"],
        example: Some(Example {
            example: "lower('ABC')",
            output: "abc",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Utf8),
        &Lower,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lower;

impl ScalarFunction for Lower {
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
        case_convert_execute(input, sel, str::to_lowercase, output)
    }
}

pub const FUNCTION_SET_UPPER: ScalarFunctionSet = ScalarFunctionSet {
    name: "upper",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Convert the string to uppercase.",
        arguments: &["string"],
        example: Some(Example {
            example: "upper('ABC')",
            output: "ABC",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Utf8),
        &Upper,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Upper;

impl ScalarFunction for Upper {
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
        case_convert_execute(input, sel, str::to_uppercase, output)
    }
}

// TODO: Reusable string buffer.
fn case_convert_execute<F>(
    input: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    case_fn: F,
    output: &mut Array,
) -> Result<()>
where
    F: Fn(&str) -> String,
{
    UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
        input,
        sel,
        OutBuffer::from_array(output)?,
        |v, buf| buf.put(&case_fn(v)),
    )
}
