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

pub const FUNCTION_SET_LEFT: ScalarFunctionSet = ScalarFunctionSet {
    name: "left",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Get the leftmost N characters of a string.",
        arguments: &["string", "count"],
        example: Some(Example {
            example: "left('alphabet', 3)",
            output: "alp",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8, DataTypeId::Int64], DataTypeId::Utf8),
        &Left,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Left;

impl ScalarFunction for Left {
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
            |s, &count, buf| buf.put(left(s, count)),
        )
    }
}

fn left(s: &str, count: i64) -> &str {
    if count == 0 {
        return "";
    }

    if count < 0 {
        let abs_count = (-count) as usize;
        let char_count = s.chars().count();

        if abs_count >= char_count {
            return "";
        }

        let take_count = char_count - abs_count;
        match s.char_indices().nth(take_count) {
            Some((pos, _)) => &s[..pos],
            None => s,
        }
    } else {
        let count = count as usize;
        match s.char_indices().nth(count) {
            Some((pos, _)) => &s[..pos],
            None => s, // Return the entire string if count is larger than the string length
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn left_cases() {
        let test_cases = [
            (("hello", 3), "hel"),
            (("hello", 5), "hello"),
            (("hello", 10), "hello"),
            (("hello", 0), ""),
            (("hello", -5), ""),
            (("😀🙂😊", 2), "😀🙂"),
            (("", 3), ""),
        ];

        for case in test_cases {
            let out = left(case.0.0, case.0.1);
            assert_eq!(case.1, out);
        }
    }
}
