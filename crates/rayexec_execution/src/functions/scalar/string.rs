use super::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use rayexec_bullet::array::Array;
use rayexec_bullet::array::{VarlenArray, VarlenValuesBuffer};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_error::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Repeat;

impl FunctionInfo for Repeat {
    fn name(&self) -> &'static str {
        "repeat"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Utf8,
            },
            Signature {
                input: &[DataTypeId::LargeUtf8, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::LargeUtf8,
            },
        ]
    }
}

impl ScalarFunction for Repeat {
    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(RepeatUtf8Impl::deserialize(deserializer)?))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Utf8, DataType::Int64) => Ok(Box::new(RepeatUtf8Impl { large: false })),
            (DataType::LargeUtf8, DataType::Int64) => Ok(Box::new(RepeatUtf8Impl { large: true })),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepeatUtf8Impl {
    large: bool,
}

impl PlannedScalarFunction for RepeatUtf8Impl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Repeat
    }

    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        self
    }

    fn return_type(&self) -> DataType {
        if self.large {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        }
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let strings = arrays[0];
        let nums = arrays[1];
        Ok(match (strings.as_ref(), nums.as_ref()) {
            (Array::Utf8(strings), Array::Int64(nums)) => {
                let mut buffer = VarlenValuesBuffer::default();
                let validity = BinaryExecutor::execute(
                    strings,
                    nums,
                    |s, count| s.repeat(count as usize),
                    &mut buffer,
                )?;
                Array::Utf8(VarlenArray::new(buffer, validity))
            }
            (Array::LargeUtf8(strings), Array::Int64(nums)) => {
                let mut buffer = VarlenValuesBuffer::default();
                let validity = BinaryExecutor::execute(
                    strings,
                    nums,
                    |s, count| s.repeat(count as usize),
                    &mut buffer,
                )?;
                Array::LargeUtf8(VarlenArray::new(buffer, validity))
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}
