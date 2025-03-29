use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalI64, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_STRPOS: ScalarFunctionSet = ScalarFunctionSet {
    name: "strpos",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Returns the position of a substring within a string. Returns 0 if the substring is not found.",
        arguments: &["string", "substring"],
        example: Some(Example {
            example: "strpos('hello', 'll')",
            output: "3",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Int64),
        &Strpos,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Strpos;

impl ScalarFunction for Strpos {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Int64,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalI64, _>(
            &input.arrays()[0],
            sel,
            &input.arrays()[1],
            sel,
            OutBuffer::from_array(output)?,
            |s, substring, buf| buf.put(&strpos(s, substring)),
        )
    }
}

fn strpos(s: &str, substring: &str) -> i64 {
    s.match_indices(substring)
        .next()
        .map(|(byte_idx, _)| s[..byte_idx].chars().count() + 1)
        .unwrap_or(0) as i64 // Return 1-based index, 0 if not exists
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strpos_cases() {
        let test_cases = [
            (("hello", "ll"), 3),
            (("hello", "hello"), 1),
            (("hello", "o"), 5),
            (("hello", "x"), 0),
            (("hello", ""), 1),
            (("", "hello"), 0),
            (("", ""), 1),
            (("😀🙂😊", "🙂"), 2),
        ];

        for case in test_cases {
            let out = strpos(case.0.0, case.0.1);
            assert_eq!(case.1, out, "case: {case:?}");
        }
    }
}
