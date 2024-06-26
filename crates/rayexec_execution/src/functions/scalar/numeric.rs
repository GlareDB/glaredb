use super::{GenericScalarFunction, SpecializedScalarFunction};
use crate::functions::scalar::macros::{primitive_unary_execute, primitive_unary_execute_bool};
use crate::functions::{
    invalid_input_types_error, specialize_check_num_args, FunctionInfo, Signature,
};
use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_error::Result;
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
                return_type: DataTypeId::Boolean,
            },
            Signature {
                input: &[DataTypeId::Float64],
                return_type: DataTypeId::Boolean,
            },
        ]
    }
}

impl GenericScalarFunction for IsNan {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 | DataType::Float64 => Ok(Box::new(IsNanSpecialized)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNanSpecialized;

impl SpecializedScalarFunction for IsNanSpecialized {
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
                return_type: DataTypeId::Float32,
            },
            Signature {
                input: &[DataTypeId::Float64],
                return_type: DataTypeId::Float64,
            },
        ]
    }
}

impl GenericScalarFunction for Ceil {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 | DataType::Float64 => Ok(Box::new(CeilSpecialized)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CeilSpecialized;

impl SpecializedScalarFunction for CeilSpecialized {
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
                return_type: DataTypeId::Float32,
            },
            Signature {
                input: &[DataTypeId::Float64],
                return_type: DataTypeId::Float64,
            },
        ]
    }
}

impl GenericScalarFunction for Floor {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 | DataType::Float64 => Ok(Box::new(FloorSpecialized)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorSpecialized;

impl SpecializedScalarFunction for FloorSpecialized {
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
