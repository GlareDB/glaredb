use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::{PhysicalI64, PhysicalUtf8};
use rayexec_bullet::executor::scalar::{BinaryExecutor, TernaryExecutor};
use rayexec_error::Result;

use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{
    invalid_input_types_error,
    plan_check_num_args_one_of,
    FunctionInfo,
    Signature,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeftPad;

impl FunctionInfo for LeftPad {
    fn name(&self) -> &'static str {
        "lpad"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Utf8,
            },
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Int64, DataTypeId::Utf8],
                variadic: None,
                return_type: DataTypeId::Utf8,
            },
        ]
    }
}

impl ScalarFunction for LeftPad {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(LeftPadImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args_one_of(self, inputs, [2, 3])?;

        match inputs.len() {
            2 => match (&inputs[0], &inputs[1]) {
                (DataType::Utf8, DataType::Int64) => Ok(Box::new(LeftPadImpl)),
                (a, b) => Err(invalid_input_types_error(self, &[a, b])),
            },
            3 => match (&inputs[0], &inputs[1], &inputs[2]) {
                (DataType::Utf8, DataType::Int64, DataType::Utf8) => Ok(Box::new(LeftPadImpl)),
                (a, b, c) => Err(invalid_input_types_error(self, &[a, b, c])),
            },
            other => unreachable!("num inputs checked, got {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeftPadImpl;

impl PlannedScalarFunction for LeftPadImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &LeftPad
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let mut string_buf = String::new();
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(inputs[0].logical_len()),
        };

        match inputs.len() {
            2 => BinaryExecutor::execute::<PhysicalUtf8, PhysicalI64, _, _>(
                inputs[0],
                inputs[1],
                builder,
                |s, count, buf| {
                    lpad(s, count, " ", &mut string_buf);
                    buf.put(&string_buf);
                },
            ),
            3 => TernaryExecutor::execute::<PhysicalUtf8, PhysicalI64, PhysicalUtf8, _, _>(
                inputs[0],
                inputs[1],
                inputs[2],
                builder,
                |s, count, pad, buf| {
                    lpad(s, count, pad, &mut string_buf);
                    buf.put(&string_buf);
                },
            ),
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
                input: &[DataTypeId::Utf8, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Utf8,
            },
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Int64, DataTypeId::Utf8],
                variadic: None,
                return_type: DataTypeId::Utf8,
            },
        ]
    }
}

impl ScalarFunction for RightPad {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(RightPadImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args_one_of(self, inputs, [2, 3])?;

        match inputs.len() {
            2 => match (&inputs[0], &inputs[1]) {
                (DataType::Utf8, DataType::Int64) => Ok(Box::new(RightPadImpl)),
                (a, b) => Err(invalid_input_types_error(self, &[a, b])),
            },
            3 => match (&inputs[0], &inputs[1], &inputs[2]) {
                (DataType::Utf8, DataType::Int64, DataType::Utf8) => Ok(Box::new(RightPadImpl)),
                (a, b, c) => Err(invalid_input_types_error(self, &[a, b, c])),
            },
            other => unreachable!("num inputs checked, got {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RightPadImpl;

impl PlannedScalarFunction for RightPadImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &RightPad
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let mut string_buf = String::new();
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(inputs[0].logical_len()),
        };

        match inputs.len() {
            2 => BinaryExecutor::execute::<PhysicalUtf8, PhysicalI64, _, _>(
                inputs[0],
                inputs[1],
                builder,
                |s, count, buf| {
                    rpad(s, count, " ", &mut string_buf);
                    buf.put(&string_buf);
                },
            ),
            3 => TernaryExecutor::execute::<PhysicalUtf8, PhysicalI64, PhysicalUtf8, _, _>(
                inputs[0],
                inputs[1],
                inputs[2],
                builder,
                |s, count, pad, buf| {
                    rpad(s, count, pad, &mut string_buf);
                    buf.put(&string_buf);
                },
            ),
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
