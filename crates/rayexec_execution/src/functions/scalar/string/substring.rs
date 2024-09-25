use std::sync::Arc;

use rayexec_bullet::{
    array::{Array, VarlenArray, VarlenValuesBuffer},
    datatype::{DataType, DataTypeId},
    executor::scalar::{BinaryExecutor, TernaryExecutor},
};
use rayexec_error::Result;

use crate::functions::{
    exec_invalid_array_type_err, invalid_input_types_error, plan_check_num_args_one_of,
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

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        if inputs.len() == 2 {
            let strings = match inputs[0].as_ref() {
                Array::Utf8(arr) => arr,
                other => return Err(exec_invalid_array_type_err(self, other)),
            };

            let from = match inputs[1].as_ref() {
                Array::Int64(arr) => arr,
                other => return Err(exec_invalid_array_type_err(self, other)),
            };

            let mut values = VarlenValuesBuffer::<i32>::default();
            let validity = BinaryExecutor::execute(strings, from, substring_from, &mut values)?;

            Ok(Array::Utf8(VarlenArray::new(values, validity)))
        } else {
            let strings = match inputs[0].as_ref() {
                Array::Utf8(arr) => arr,
                other => return Err(exec_invalid_array_type_err(self, other)),
            };

            let from = match inputs[1].as_ref() {
                Array::Int64(arr) => arr,
                other => return Err(exec_invalid_array_type_err(self, other)),
            };

            let count = match inputs[2].as_ref() {
                Array::Int64(arr) => arr,
                other => return Err(exec_invalid_array_type_err(self, other)),
            };

            let mut values = VarlenValuesBuffer::<i32>::default();
            let validity =
                TernaryExecutor::execute(strings, from, count, substring_from_count, &mut values)?;

            Ok(Array::Utf8(VarlenArray::new(values, validity)))
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
