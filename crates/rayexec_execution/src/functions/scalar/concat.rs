use std::sync::Arc;

use rayexec_bullet::{
    array::{Array, Utf8Array, VarlenValuesBuffer},
    datatype::{DataType, DataTypeId},
    executor::scalar::UniformExecutor,
};
use rayexec_error::{RayexecError, Result};

use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};

use super::{PlannedScalarFunction, ScalarFunction};

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
    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        for input in inputs {
            if input.datatype_id() != DataTypeId::Utf8 {
                return Err(invalid_input_types_error(self, inputs));
            }
        }

        Ok(Box::new(StringConcatImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringConcatImpl;

impl PlannedScalarFunction for StringConcatImpl {
    fn name(&self) -> &'static str {
        "string_concat_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        if inputs.is_empty() {
            return Ok(Array::Utf8(Utf8Array::from(vec![String::new()])));
        }

        let string_arrs = inputs
            .iter()
            .map(|arr| match arr.as_ref() {
                Array::Utf8(arr) => Ok(arr),
                other => Err(RayexecError::new(format!(
                    "Expected Utf8 arrays, got {}",
                    other.datatype(),
                ))),
            })
            .collect::<Result<Vec<_>>>()?;

        let mut values = VarlenValuesBuffer::default();

        // TODO: Reusable buffer?
        let validity = UniformExecutor::execute(&string_arrs, |strs| strs.join(""), &mut values)?;

        Ok(Array::Utf8(Utf8Array::new(values, validity)))
    }
}
