use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::{BinaryExecutor, UniformExecutor};
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use super::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};

// TODO: Currently '||' aliases to this, however there should be two separate
// concat functions. One that should return null on any null arguments (||), and
// one that should omit null arguments when concatenating (the normal concat).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Concat;

impl FunctionInfo for Concat {
    fn name(&self) -> &'static str {
        "concat"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[],
            variadic: Some(DataTypeId::Utf8),
            return_type: DataTypeId::Utf8,
        }]
    }
}

impl ScalarFunction for Concat {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(StringConcatImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        for input in inputs {
            if input.datatype_id() != DataTypeId::Utf8 {
                return Err(invalid_input_types_error(self, inputs));
            }
        }

        Ok(Box::new(StringConcatImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StringConcatImpl;

impl PlannedScalarFunction for StringConcatImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Concat
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        match inputs.len() {
            0 => {
                let mut array = Array::from_iter([""]);
                array.set_physical_validity(0, false);
                Ok(array)
            }
            1 => Ok(inputs[0].clone()),
            2 => {
                let a = inputs[0];
                let b = inputs[1];

                let mut string_buf = String::new();

                // TODO: Compute data capacity.

                BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype: DataType::Utf8,
                        buffer: GermanVarlenBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| {
                        string_buf.clear();
                        string_buf.push_str(a);
                        string_buf.push_str(b);
                        buf.put(string_buf.as_str());
                    },
                )
            }
            _ => {
                let mut string_buf = String::new();

                UniformExecutor::execute::<PhysicalUtf8, _, _>(
                    inputs,
                    ArrayBuilder {
                        datatype: DataType::Utf8,
                        buffer: GermanVarlenBuffer::with_len(inputs[0].logical_len()),
                    },
                    |strings, buf| {
                        string_buf.clear();
                        for s in strings {
                            string_buf.push_str(s);
                        }
                        buf.put(string_buf.as_str());
                    },
                )
            }
        }
    }
}
