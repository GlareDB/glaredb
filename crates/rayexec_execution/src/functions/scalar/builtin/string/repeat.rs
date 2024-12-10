use std::fmt::Debug;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::{PhysicalI64, PhysicalUtf8};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use crate::functions::scalar::{PlannedScalarFunction2, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Repeat;

impl FunctionInfo for Repeat {
    fn name(&self) -> &'static str {
        "repeat"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Utf8, DataTypeId::Int64],
            variadic: None,
            return_type: DataTypeId::Utf8,
        }]
    }
}

impl ScalarFunction for Repeat {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(RepeatUtf8Impl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Utf8, DataType::Int64) => Ok(Box::new(RepeatUtf8Impl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepeatUtf8Impl;

impl PlannedScalarFunction2 for RepeatUtf8Impl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Repeat
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let strings = inputs[0];
        let nums = inputs[1];

        // TODO: Capacity

        let mut string_buf = String::new();

        BinaryExecutor::execute::<PhysicalUtf8, PhysicalI64, _, _>(
            strings,
            nums,
            ArrayBuilder {
                datatype: DataType::Utf8,
                buffer: GermanVarlenBuffer::with_len(strings.logical_len()),
            },
            |s, num, buf| {
                string_buf.clear();
                for _ in 0..num {
                    string_buf.push_str(s);
                }
                buf.put(string_buf.as_str())
            },
        )
    }
}
