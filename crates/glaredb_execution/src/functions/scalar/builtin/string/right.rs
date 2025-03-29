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

pub const FUNCTION_SET_RIGHT: ScalarFunctionSet = ScalarFunctionSet {
    name: "right",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Get the rightmost N characters of a string.",
        arguments: &["string", "count"],
        example: Some(Example {
            example: "right('alphabet', 3)",
            output: "bet",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8, DataTypeId::Int64], DataTypeId::Utf8),
        &Right,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Right;

impl ScalarFunction for Right {
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

        BinaryExecutor::execute::<PhysicalUtf8, PhysicalI64, PhysicalUtf8, _>(
            &input.arrays()[0],
            sel,
            &input.arrays()[1],
            sel,
            OutBuffer::from_array(output)?,
            |s, &count, buf| buf.put(right(s, count)),
        )
    }
}

fn right(s: &str, count: i64) -> &str {
    if count == 0 {
        return "";
    }

    if count < 0 {
        let abs_count = (-count) as usize;

        if abs_count >= s.chars().count() {
            return "";
        }

        match s.char_indices().nth(abs_count) {
            Some((pos, _)) => &s[pos..],
            None => "",
        }
    } else {
        let count = count as usize;
        let char_count = s.chars().count();

        if count >= char_count {
            return s;
        }

        let pos_to_skip = char_count - count;
        match s.char_indices().nth(pos_to_skip) {
            Some((pos, _)) => &s[pos..],
            None => s,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn right_cases() {
        let test_cases = [
            (("hello", 3), "llo"),
            (("hello", 5), "hello"),
            (("hello", 10), "hello"),
            (("hello", 0), ""),
            (("hello", -3), "lo"),  // Negative case starts from left
            (("hello", -2), "llo"), // Skip 2 chars from left
            (("😀🙂😊", 2), "🙂😊"),
            (("", 3), ""),
        ];

        for case in test_cases {
            let out = right(case.0.0, case.0.1);
            assert_eq!(case.1, out);
        }
    }
}
