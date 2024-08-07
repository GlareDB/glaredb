use crate::functions::{plan_check_num_args, FunctionInfo, Signature};
use rayexec_bullet::array::{Array, PrimitiveArray};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_error::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{PlannedScalarFunction, ScalarFunction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Random;

impl FunctionInfo for Random {
    fn name(&self) -> &'static str {
        "random"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl ScalarFunction for Random {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(RandomImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 0)?;
        Ok(Box::new(RandomImpl))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RandomImpl;

impl PlannedScalarFunction for RandomImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Random
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Float64
    }

    fn execute(&self, _arrays: &[&Arc<Array>]) -> Result<Array> {
        let val = rand::random::<f64>();
        Ok(Array::Float64(PrimitiveArray::new(vec![val], None)))
    }
}
