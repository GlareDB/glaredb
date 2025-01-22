use rayexec_error::Result;

use crate::arrays::array::physical_type::{PhysicalI64, PhysicalUtf8};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::{BinaryExecutor, TernaryExecutor};
use crate::arrays::executor::OutBuffer;
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
pub struct LeftPad;

impl FunctionInfo for LeftPad {
    fn name(&self) -> &'static str {
        "lpad"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Utf8,
                doc: Some(&Documentation{
                    category: Category::String,
                    description: "Left pad a string with spaces until the resulting string contains 'count' characters.",
                    arguments: &["string", "count"],
                    example: Some(Example{
                        example: "lpad('house', 8)",
                        output: "   house",
                    }),
                }),
            },
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Int64, DataTypeId::Utf8],
                variadic_arg: None,
                return_type: DataTypeId::Utf8,
                doc: Some(&Documentation{
                    category: Category::String,
                    description: "Left pad a string with another string until the resulting string contains 'count' characters.",
                    arguments: &["string", "count", "pad"],
                    example: Some(Example{
                        example: "lpad('house', 8, '_')",
                        output: "___house",
                    }),
                }),
            },
        ]
    }
}

impl ScalarFunction for LeftPad {
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

        match inputs.len() {
            2 => match (&datatypes[0], &datatypes[1]) {
                (DataType::Utf8, DataType::Int64) => (),
                (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
            },
            3 => match (&datatypes[0], &datatypes[1], &datatypes[2]) {
                (DataType::Utf8, DataType::Int64, DataType::Utf8) => (),
                (a, b, c) => return Err(invalid_input_types_error(self, &[a, b, c])),
            },
            other => unreachable!("num inputs checked, got {other}"),
        }

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Utf8,
            inputs,
            function_impl: Box::new(LeftPadImpl),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeftPadImpl;

impl ScalarFunctionImpl for LeftPadImpl {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        let mut string_buf = String::new();

        match input.arrays().len() {
            2 => BinaryExecutor::execute::<PhysicalUtf8, PhysicalI64, PhysicalUtf8, _>(
                &input.arrays[0],
                sel,
                &input.arrays()[1],
                sel,
                OutBuffer::from_array(output)?,
                |s, &count, buf| {
                    lpad(s, count, " ", &mut string_buf);
                    buf.put(&string_buf);
                },
            ),
            3 => {
                TernaryExecutor::execute::<PhysicalUtf8, PhysicalI64, PhysicalUtf8, PhysicalUtf8, _>(
                    &input.arrays[0],
                    sel,
                    &input.arrays()[1],
                    sel,
                    &input.arrays()[2],
                    sel,
                    OutBuffer::from_array(output)?,
                    |s, &count, pad, buf| {
                        lpad(s, count, pad, &mut string_buf);
                        buf.put(&string_buf);
                    },
                )
            }
            other => unreachable!("num inputs checked, got {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RightPad;

impl FunctionInfo for RightPad {
    fn name(&self) -> &'static str {
        "rpad"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Utf8,
                doc: Some(&Documentation{
                    category: Category::String,
                    description: "Right pad a string with spaces until the resulting string contains 'count' characters.",
                    arguments: &["string", "count"],
                    example: Some(Example{
                        example: "rpad('house', 8)",
                        output: "house   ",
                    }),
                }),
            },
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Int64, DataTypeId::Utf8],
                variadic_arg: None,
                return_type: DataTypeId::Utf8,
                doc: Some(&Documentation{
                    category: Category::String,
                    description: "Right pad a string with another string until the resulting string contains 'count' characters.",
                    arguments: &["string", "count", "pad"],
                    example: Some(Example{
                        example: "rpad('house', 8, '_')",
                        output: "house___",
                    }),
                }),

            },
        ]
    }
}

impl ScalarFunction for RightPad {
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

        match inputs.len() {
            2 => match (&datatypes[0], &datatypes[1]) {
                (DataType::Utf8, DataType::Int64) => (),
                (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
            },
            3 => match (&datatypes[0], &datatypes[1], &datatypes[2]) {
                (DataType::Utf8, DataType::Int64, DataType::Utf8) => (),
                (a, b, c) => return Err(invalid_input_types_error(self, &[a, b, c])),
            },
            other => unreachable!("num inputs checked, got {other}"),
        }

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Utf8,
            inputs,
            function_impl: Box::new(RightPadImpl),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RightPadImpl;

impl ScalarFunctionImpl for RightPadImpl {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        let mut string_buf = String::new();

        match input.arrays().len() {
            2 => BinaryExecutor::execute::<PhysicalUtf8, PhysicalI64, PhysicalUtf8, _>(
                &input.arrays[0],
                sel,
                &input.arrays()[1],
                sel,
                OutBuffer::from_array(output)?,
                |s, &count, buf| {
                    rpad(s, count, " ", &mut string_buf);
                    buf.put(&string_buf);
                },
            ),
            3 => {
                TernaryExecutor::execute::<PhysicalUtf8, PhysicalI64, PhysicalUtf8, PhysicalUtf8, _>(
                    &input.arrays[0],
                    sel,
                    &input.arrays()[1],
                    sel,
                    &input.arrays()[2],
                    sel,
                    OutBuffer::from_array(output)?,
                    |s, &count, pad, buf| {
                        rpad(s, count, pad, &mut string_buf);
                        buf.put(&string_buf);
                    },
                )
            }
            other => unreachable!("num inputs checked, got {other}"),
        }
    }
}

fn lpad(s: &str, count: i64, pad: &str, buf: &mut String) {
    buf.clear();

    if pad.is_empty() {
        // Just write the original string and don't pad. Matches postgres.
        buf.push_str(s);
        return;
    }

    let s_char_len = s.chars().count() as i64;
    if s_char_len > count {
        // Just write count number of chars to output.
        buf.push_str(&s.chars().as_str()[..count as usize]);
        return;
    }

    let pad_char_len = pad.chars().count() as i64;
    let mut rem = count - s_char_len;
    while rem > 0 {
        buf.push_str(pad);
        rem -= pad_char_len;
    }

    if rem < 0 {
        // Need to trim from right now.
        let byte_pos = buf
            .char_indices()
            .rev()
            .skip((rem.abs() - 1) as usize)
            .map(|(pos, _)| pos)
            .next();

        if let Some(pos) = byte_pos {
            buf.truncate(pos);
        }
    }

    // Push original string.
    buf.push_str(s);
}

fn rpad(s: &str, count: i64, pad: &str, buf: &mut String) {
    buf.clear();
    buf.push_str(s);

    if pad.is_empty() {
        return;
    }

    let s_char_len = s.chars().count() as i64;
    let pad_char_len = pad.chars().count() as i64;
    let mut rem = count - s_char_len;

    while rem > 0 {
        buf.push_str(pad);
        rem -= pad_char_len;
    }

    if rem < 0 {
        // Need to trim from right now.
        let byte_pos = buf
            .char_indices()
            .rev()
            .skip((rem.abs() - 1) as usize)
            .map(|(pos, _)| pos)
            .next();

        if let Some(pos) = byte_pos {
            buf.truncate(pos);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestCase {
        s: &'static str,
        count: i64,
        pad: &'static str,
        expected: &'static str,
    }

    #[test]
    fn lpad_cases() {
        let cases = [
            TestCase {
                s: "aaa",
                pad: "b",
                count: 5,
                expected: "bbaaa",
            },
            TestCase {
                s: "aaa",
                pad: "b",
                count: 2,
                expected: "aa",
            },
            TestCase {
                s: "aaa",
                pad: "bb",
                count: 6,
                expected: "bbbaaa",
            },
            TestCase {
                s: "aaa",
                pad: "",
                count: 6,
                expected: "aaa",
            },
        ];

        let mut buf = String::new();
        for case in cases {
            lpad(case.s, case.count, case.pad, &mut buf);
            assert_eq!(case.expected, buf);
        }
    }

    #[test]
    fn rpad_cases() {
        let cases = [
            TestCase {
                s: "aaa",
                pad: "b",
                count: 5,
                expected: "aaabb",
            },
            TestCase {
                s: "aaa",
                pad: "b",
                count: 2,
                expected: "aa",
            },
            TestCase {
                s: "aaa",
                pad: "bb",
                count: 6,
                expected: "aaabbb",
            },
            TestCase {
                s: "aaa",
                pad: "",
                count: 6,
                expected: "aaa",
            },
        ];

        let mut buf = String::new();
        for case in cases {
            rpad(case.s, case.count, case.pad, &mut buf);
            assert_eq!(case.expected, buf);
        }
    }
}
