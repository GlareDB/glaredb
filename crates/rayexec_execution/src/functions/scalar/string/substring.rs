use rayexec_bullet::{
    array::Array,
    datatype::{DataType, DataTypeId},
    executor::{
        builder::{ArrayBuilder, GermanVarlenBuffer},
        physical_type::{PhysicalI64, PhysicalUtf8},
        scalar::{BinaryExecutor, TernaryExecutor},
    },
};
use rayexec_error::{RayexecError, Result};

use crate::functions::{
    invalid_input_types_error, plan_check_num_args_one_of,
    scalar::{PlannedScalarFunction, ScalarFunction},
    FunctionInfo, Signature,
};

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
                input: &[DataTypeId::Utf8, DataTypeId::Int64, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Utf8,
            },
            // substring(<string>, <from>)
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Utf8,
            },
        ]
    }
}

impl ScalarFunction for Substring {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(SubstringImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args_one_of(self, inputs, [2, 3])?;

        if inputs.len() == 2 {
            match (&inputs[0], &inputs[1]) {
                (DataType::Utf8, DataType::Int64) => Ok(Box::new(SubstringImpl)),
                (a, b) => Err(invalid_input_types_error(self, &[a, b])),
            }
        } else {
            match (&inputs[0], &inputs[1], &inputs[2]) {
                (DataType::Utf8, DataType::Int64, DataType::Int64) => Ok(Box::new(SubstringImpl)),
                (a, b, c) => Err(invalid_input_types_error(self, &[a, b, c])),
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubstringImpl;

impl PlannedScalarFunction for SubstringImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Substring
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        // TODO: Capacity
        // TODO: Also would be possible to use the same underlying storage.
        match inputs.len() {
            2 => {
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
            3 => {
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
            other => Err(RayexecError::new(format!(
                "Unexpected array count: {other}"
            ))),
        }
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
