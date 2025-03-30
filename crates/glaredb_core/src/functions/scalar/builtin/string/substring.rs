use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalI64, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::{BinaryExecutor, TernaryExecutor};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_SUBSTRING: ScalarFunctionSet = ScalarFunctionSet {
    name: "substring",
    aliases: &["substr"],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Get a substring of a string starting at an index until the end of the string. The index is 1-based.",
        arguments: &["string", "index"],
        example: Some(Example {
            example: "substring('alphabet', 3)",
            output: "phabet",
        }),
    }),
    functions: &[
        // substring(<string>, <from>)
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Utf8, DataTypeId::Int64], DataTypeId::Utf8),
            &SubstringFrom,
        ),
        // substring(<string>, <from>, <to>)
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Utf8, DataTypeId::Int64, DataTypeId::Int64],
                DataTypeId::Utf8,
            ),
            &SubstringFromTo,
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct SubstringFrom;

impl ScalarFunction for SubstringFrom {
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
            |s, &from, buf| buf.put(substring_from(s, from)),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SubstringFromTo;

impl ScalarFunction for SubstringFromTo {
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

        TernaryExecutor::execute::<PhysicalUtf8, PhysicalI64, PhysicalI64, PhysicalUtf8, _>(
            &input.arrays()[0],
            sel,
            &input.arrays()[1],
            sel,
            &input.arrays()[2],
            sel,
            OutBuffer::from_array(output)?,
            |s, &from, &count, buf| buf.put(substring_from_count(s, from, count)),
        )
    }
}

fn substring_from(s: &str, from: i64) -> &str {
    let start = (from - 1) as usize;
    let mut chars = s.chars();
    for _ in 0..start {
        let _ = chars.next();
    }

    chars.as_str()
}

fn substring_from_count(s: &str, from: i64, count: i64) -> &str {
    let start = (from - 1) as usize;
    let count = count as usize;

    let mut chars = s.chars();
    for _ in 0..start {
        let _ = chars.next();
    }

    // Determine byte position for count if we're dealing with utf8.
    let byte_pos = chars
        .as_str()
        .char_indices()
        .skip(count)
        .map(|(pos, _)| pos)
        .next();

    match byte_pos {
        Some(pos) => &chars.as_str()[..pos],
        None => {
            // End is beyond length of string, return full string.
            chars.as_str()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substring_from_cases() {
        // ((string, from), expected)
        let test_cases = [
            (("hello", 1), "hello"),
            (("hello", 2), "ello"),
            (("hello", 3), "llo"),
            (("hello", 8), ""),
        ];

        for case in test_cases {
            let out = substring_from(case.0.0, case.0.1);
            assert_eq!(case.1, out);
        }
    }

    #[test]
    fn substring_from_count_cases() {
        // ((string, from, count), expected)
        let test_cases = [
            (("hello", 1, 10), "hello"),
            (("hello", 2, 10), "ello"),
            (("hello", 3, 10), "llo"),
            (("hello", 8, 10), ""),
            (("hello", 1, 0), ""),
            (("hello", 1, 2), "he"),
            (("hello", 1, 3), "hel"),
            (("hello", 2, 2), "el"),
            (("hello", 2, 4), "ello"),
            (("hello", 2, 5), "ello"),
        ];

        for case in test_cases {
            let out = substring_from_count(case.0.0, case.0.1, case.0.2);
            assert_eq!(case.1, out);
        }
    }
}
