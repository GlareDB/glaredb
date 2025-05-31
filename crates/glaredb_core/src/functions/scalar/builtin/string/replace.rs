use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::PhysicalUtf8;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::TernaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_REPLACE: ScalarFunctionSet = ScalarFunctionSet {
    name: "replace",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::String,
        description: "Replace all occurrences in string of substring from with substring to.",
        arguments: &["string", "from", "to"],
        example: Some(Example {
            example: "replace('abcdefabcdef', 'cd', 'XX')",
            output: "abXXefabXXef",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(
            &[DataTypeId::Utf8, DataTypeId::Utf8, DataTypeId::Utf8],
            DataTypeId::Utf8,
        ),
        &Replace,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Replace;

impl ScalarFunction for Replace {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::utf8(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        TernaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, _>(
            &input.arrays()[0],
            sel,
            &input.arrays()[1],
            sel,
            &input.arrays()[2],
            sel,
            OutBuffer::from_array(output)?,
            |s, from, to, buf| {
                if from.is_empty() {
                    buf.put(s);
                } else {
                    buf.put(&s.replace(from, to));
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn replace_cases() {
        let test_cases = [
            (("abcdefabcdef", "cd", "XX"), "abXXefabXXef"),
            (("hello world", "l", "L"), "heLLo worLd"),
            (("test", "missing", "X"), "test"),
            (("", "a", "b"), ""),
            (("abc", "", "X"), "abc"),
            (("abc", "abc", ""), ""),
            (("aaaa", "aa", "b"), "bb"),
            (("abcabc", "abc", "xyz"), "xyzxyz"),
        ];

        for case in test_cases {
            let out = if case.0.1.is_empty() {
                case.0.0.to_string()
            } else {
                case.0.0.replace(case.0.1, case.0.2)
            };
            assert_eq!(case.1, out, "case: {case:?}");
        }
    }
}
