use crate::functions::scalar::macros::primitive_unary_execute;
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_error::Result;
use std::sync::Arc;

use super::{PlannedScalarFunction, ScalarFunction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Negate;

impl FunctionInfo for Negate {
    fn name(&self) -> &'static str {
        "negate"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Float32],
                variadic: None,
                return_type: DataTypeId::Float32,
            },
            Signature {
                input: &[DataTypeId::Float64],
                variadic: None,
                return_type: DataTypeId::Float64,
            },
            Signature {
                input: &[DataTypeId::Int8],
                variadic: None,
                return_type: DataTypeId::Int8,
            },
            Signature {
                input: &[DataTypeId::Int16],
                variadic: None,
                return_type: DataTypeId::Int16,
            },
            Signature {
                input: &[DataTypeId::Int32],
                variadic: None,
                return_type: DataTypeId::Int32,
            },
            Signature {
                input: &[DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Int64,
            },
            Signature {
                input: &[DataTypeId::Interval],
                variadic: None,
                return_type: DataTypeId::Interval,
            },
        ]
    }
}

impl ScalarFunction for Negate {
    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64 => Ok(Box::new(NegateImpl {
                datatype: inputs[0].clone(),
            })),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegateImpl {
    datatype: DataType,
}

impl PlannedScalarFunction for NegateImpl {
    fn name(&self) -> &'static str {
        "negate_impl"
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let first = arrays[0];
        Ok(match first.as_ref() {
            Array::Int8(input) => {
                primitive_unary_execute!(input, Int8, |a| -a)
            }
            Array::Int16(input) => {
                primitive_unary_execute!(input, Int16, |a| -a)
            }
            Array::Int32(input) => {
                primitive_unary_execute!(input, Int32, |a| -a)
            }
            Array::Int64(input) => {
                primitive_unary_execute!(input, Int64, |a| -a)
            }
            Array::Float32(input) => {
                primitive_unary_execute!(input, Float32, |a| -a)
            }
            Array::Float64(input) => {
                primitive_unary_execute!(input, Float64, |a| -a)
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}
