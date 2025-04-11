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
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_INITCAP: ScalarFunctionSet = ScalarFunctionSet {
    name: "initcap",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::String,
        description: "Convert first letter of each word to uppercase.",
        arguments: &["string"],
        example: Some(Example {
            example: "initcap('hello world')",
            output: "Hello World",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Utf8),
        &Initcap,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Initcap;

impl ScalarFunction for Initcap {
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
        initcap_execute(input, sel, output)
    }
}

fn initcap_execute(
    input: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    output: &mut Array,
) -> Result<()> {
    let mut string_buf = String::new();
    UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
        input,
        sel,
        OutBuffer::from_array(output)?,
        |s, buf| {
            string_buf.clear();
            string_buf.reserve(s.len());

            let mut capitalize_next = true;

            for c in s.chars() {
                if c.is_alphabetic() {
                    if capitalize_next {
                        string_buf.extend(c.to_uppercase());
                        capitalize_next = false;
                    } else {
                        string_buf.extend(c.to_lowercase());
                    }
                } else {
                    string_buf.push(c);
                    capitalize_next =
                        c.is_whitespace() || c == '-' || c == '_' || c == '.' || c == ',';
                }
            }

            buf.put(&string_buf);
        },
    )
}
