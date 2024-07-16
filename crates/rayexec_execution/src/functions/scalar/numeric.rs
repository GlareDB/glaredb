use super::{PlannedScalarFunction, ScalarFunction};
use crate::functions::scalar::macros::{primitive_unary_execute, primitive_unary_execute_bool};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_error::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNan;

impl FunctionInfo for IsNan {
    fn name(&self) -> &'static str {
        "isnan"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Float32],
                variadic: None,
                return_type: DataTypeId::Boolean,
            },
            Signature {
                input: &[DataTypeId::Float64],
                variadic: None,
                return_type: DataTypeId::Boolean,
            },
        ]
    }
}

impl ScalarFunction for IsNan {
    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(IsNanImpl::deserialize(deserializer)?))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 | DataType::Float64 => Ok(Box::new(IsNanImpl)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IsNanImpl;

impl PlannedScalarFunction for IsNanImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &IsNan
    }

    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        self
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let array = arrays[0];
        Ok(match array.as_ref() {
            Array::Float32(input) => {
                primitive_unary_execute_bool!(input, |f| f.is_nan())
            }
            Array::Float64(input) => {
                primitive_unary_execute_bool!(input, |f| f.is_nan())
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ceil;

impl FunctionInfo for Ceil {
    fn name(&self) -> &'static str {
        "ceil"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["ceiling"]
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
        ]
    }
}

impl ScalarFunction for Ceil {
    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(CeilImpl::deserialize(deserializer)?))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 | DataType::Float64 => Ok(Box::new(CeilImpl {
                datatype: inputs[0].clone(),
            })),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CeilImpl {
    datatype: DataType,
}

impl PlannedScalarFunction for CeilImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Ceil
    }

    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        self
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let array = arrays[0];
        Ok(match array.as_ref() {
            Array::Float32(input) => {
                primitive_unary_execute!(input, Float32, |f| f.ceil())
            }
            Array::Float64(input) => {
                primitive_unary_execute!(input, Float64, |f| f.ceil())
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Floor;

impl FunctionInfo for Floor {
    fn name(&self) -> &'static str {
        "floor"
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
        ]
    }
}

impl ScalarFunction for Floor {
    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(FloorImpl::deserialize(deserializer)?))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 | DataType::Float64 => Ok(Box::new(FloorImpl {
                datatype: inputs[0].clone(),
            })),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FloorImpl {
    datatype: DataType,
}

impl PlannedScalarFunction for FloorImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Floor
    }

    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        self
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let array = arrays[0];
        Ok(match array.as_ref() {
            Array::Float32(input) => {
                primitive_unary_execute!(input, Float32, |f| f.floor())
            }
            Array::Float64(input) => {
                primitive_unary_execute!(input, Float64, |f| f.floor())
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}
