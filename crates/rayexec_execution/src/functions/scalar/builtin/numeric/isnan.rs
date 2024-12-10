use num_traits::Float;
use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalStorage,
    PhysicalType,
};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use super::{PlannedScalarFunction2, ScalarFunction};
use crate::functions::{
    invalid_input_types_error,
    plan_check_num_args,
    unhandled_physical_types_err,
    FunctionInfo,
    Signature,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNan;

impl FunctionInfo for IsNan {
    fn name(&self) -> &'static str {
        "isnan"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Float16],
                variadic: None,
                return_type: DataTypeId::Boolean,
            },
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
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(IsNanImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 | DataType::Float64 => Ok(Box::new(IsNanImpl)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IsNanImpl;

impl PlannedScalarFunction2 for IsNanImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &IsNan
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        match input.physical_type() {
            PhysicalType::Float16 => is_nan_execute::<PhysicalF16>(input),
            PhysicalType::Float32 => is_nan_execute::<PhysicalF32>(input),
            PhysicalType::Float64 => is_nan_execute::<PhysicalF64>(input),
            other => Err(unhandled_physical_types_err(self, [other])),
        }
    }
}

fn is_nan_execute<'a, S>(input: &'a Array) -> Result<Array>
where
    S: PhysicalStorage,
    S::Type<'a>: Float,
{
    let builder = ArrayBuilder {
        datatype: DataType::Boolean,
        buffer: BooleanBuffer::with_len(input.logical_len()),
    };

    UnaryExecutor::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.is_nan()))
}
