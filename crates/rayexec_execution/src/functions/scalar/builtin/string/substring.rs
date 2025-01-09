use rayexec_error::Result;

use crate::arrays::array::physical_type::{PhysicalI64, PhysicalUtf8};
use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use crate::arrays::executor::scalar::{BinaryExecutor, TernaryExecutor};
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{
    invalid_input_types_error,
    plan_check_num_args_one_of,
    FunctionInfo,
    Signature,
};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Substring;

impl FunctionInfo for Substring {
    fn name(&self) -> &'static str {
        "substring"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["substr"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            // substring(<string>, <from>, <for>)
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Int64, DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Utf8,
                doc: Some(&Documentation{
                    category: Category::String,
                    description: "Get a substring of a string starting at an index for some number of characters. The index is 1-based.",
                    arguments: &["string", "index", "for"],
                    example: Some(Example{
                        example: "substring('alphabet', 3, 2)",
                        output: "ph",
                    })
                })
            },
            // substring(<string>, <from>)
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Utf8,
                doc: Some(&Documentation{
                    category: Category::String,
                    description: "Get a substring of a string starting at an index until the end of the string. The index is 1-based.",
                    arguments: &["string", "index"],
                    example: Some(Example{
                        example: "substring('alphabet', 3)",
                        output: "phabet",
                    }),
                }),
            },
        ]
    }
}

impl ScalarFunction for Substring {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args_one_of(self, &inputs, [2, 3])?;

        let datatypes = inputs
            .iter()
            .map(|input| input.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        match datatypes.len() {
            2 => match (&datatypes[0], &datatypes[1]) {
                (DataType::Utf8, DataType::Int64) => Ok(PlannedScalarFunction {
                    function: Box::new(*self),
                    return_type: DataType::Utf8,
                    inputs,
                    function_impl: Box::new(SubstringFromImpl),
                }),
                (a, b) => Err(invalid_input_types_error(self, &[a, b])),
            },
            3 => match (&datatypes[0], &datatypes[1], &datatypes[2]) {
                (DataType::Utf8, DataType::Int64, DataType::Int64) => Ok(PlannedScalarFunction {
                    function: Box::new(*self),
                    return_type: DataType::Utf8,
                    inputs,
                    function_impl: Box::new(SubstringFromToImpl),
                }),
                (a, b, c) => Err(invalid_input_types_error(self, &[a, b, c])),
            },
            _ => Err(invalid_input_types_error(self, &datatypes)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubstringFromImpl;

impl ScalarFunctionImpl for SubstringFromImpl {
    fn execute2(&self, inputs: &[&Array]) -> Result<Array> {
        let len = inputs[0].logical_len();
        BinaryExecutor::execute::<PhysicalUtf8, PhysicalI64, _, _>(
            inputs[0],
            inputs[1],
            ArrayBuilder {
                datatype: DataType::Utf8,
                buffer: GermanVarlenBuffer::with_len(len),
            },
            |s, from, buf| buf.put(substring_from(s, from)),
        )
    }
}

#[derive(Debug, Clone)]
pub struct SubstringFromToImpl;

impl ScalarFunctionImpl for SubstringFromToImpl {
    fn execute2(&self, inputs: &[&Array]) -> Result<Array> {
        let len = inputs[0].logical_len();
        TernaryExecutor::execute::<PhysicalUtf8, PhysicalI64, PhysicalI64, _, _>(
            inputs[0],
            inputs[1],
            inputs[2],
            ArrayBuilder {
                datatype: DataType::Utf8,
                buffer: GermanVarlenBuffer::with_len(len),
            },
            |s, from, count, buf| buf.put(substring_from_count(s, from, count)),
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
            let out = substring_from(case.0 .0, case.0 .1);
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
            let out = substring_from_count(case.0 .0, case.0 .1, case.0 .2);
            assert_eq!(case.1, out);
        }
    }
}
