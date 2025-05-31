use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalI64, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::TernaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_SPLIT_PART: ScalarFunctionSet = ScalarFunctionSet {
    name: "split_part",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::String,
        description: "Splits string at occurrences of delimiter and returns the n'th field (counting from one), or when n is negative, returns the |n|'th-from-last field.",
        arguments: &["string", "delimiter", "n"],
        example: Some(Example {
            example: "split_part('abc~@~def~@~ghi', '~@~', 2)",
            output: "def",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(
            &[DataTypeId::Utf8, DataTypeId::Utf8, DataTypeId::Int64],
            DataTypeId::Utf8,
        ),
        &SplitPart,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct SplitPart;

impl ScalarFunction for SplitPart {
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

        TernaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalI64, PhysicalUtf8, _>(
            &input.arrays()[0],
            sel,
            &input.arrays()[1],
            sel,
            &input.arrays()[2],
            sel,
            OutBuffer::from_array(output)?,
            |s, delimiter, &n, buf| buf.put(split_part(s, delimiter, n)),
        )
    }
}

fn split_part<'a>(s: &'a str, delimiter: &str, n: i64) -> &'a str {
    if delimiter.is_empty() {
        return if n == 1 { s } else { "" };
    }

    if n > 0 {
        // From the start.
        return s.split(delimiter).nth(n as usize - 1).unwrap_or("");
    }

    if n < 0 {
        // From the end.
        let n = (-n) as usize;
        return s.rsplit(delimiter).nth(n - 1).unwrap_or("");
    }

    // n==0, empty string
    ""
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_part_cases() {
        let test_cases = [
            (("abc~@~def~@~ghi", "~@~", 1), "abc"),
            (("abc~@~def~@~ghi", "~@~", 2), "def"),
            (("abc~@~def~@~ghi", "~@~", 3), "ghi"),
            (("abc~@~def~@~ghi", "~@~", 4), ""),
            (("abc,def,ghi,jkl", ",", -1), "jkl"),
            (("abc,def,ghi,jkl", ",", -2), "ghi"),
            (("abc,def,ghi,jkl", ",", -4), "abc"),
            (("abc,def,ghi,jkl", ",", -5), ""),
            (("hello", ",", 1), "hello"),
            (("hello", ",", 2), ""),
            (("", ",", 1), ""),
            (("abc", "", 1), "abc"),
            (("abc", "", 2), ""),
            (("a,b,c", ",", 0), ""),
            (("a,,c", ",", 1), "a"),
            (("a,,c", ",", 2), ""),
            (("a,,c", ",", 3), "c"),
        ];

        for case in test_cases {
            let out = split_part(case.0.0, case.0.1, case.0.2);
            assert_eq!(case.1, out, "case: {case:?}");
        }
    }
}
